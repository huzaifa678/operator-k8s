"""
ELT pipeline: raw articles CSV → cleaned, typed Parquet on the data lake.

Extract  – read raw CSV from S3 (schema-on-read, permissive of dirty input)
Transform– normalise dates (RFC-822 / ISO / year-only), lower-case source,
           drop rows missing title/source, derive published_date partition
Load     – write Parquet partitioned by published_date

Filename kept as elt_articles.py for SparkJob path stability — the domain is
articles now, the "orders" name is historical.

Usage (spark-submit):
  elt_articles.py \
    --source-base s3a://bucket/raw/articles/ \      # YYYY/MM/DD appended
    --dest-path   s3a://bucket/processed/articles/ \
    [--date 2026-06-05]                              # defaults to today UTC
    [--source-path s3a://.../raw/articles/2026/06/05/]  # overrides base+date
    [--partition-by published_date]
"""

import argparse
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


RAW_SCHEMA = StructType([
    StructField("id",           StringType(), nullable=False),
    StructField("source",       StringType(), nullable=False),
    StructField("title",        StringType(), nullable=True),
    StructField("link",         StringType(), nullable=True),
    StructField("published_at", StringType(), nullable=True),
    StructField("summary",      StringType(), nullable=True),
    StructField("category",     StringType(), nullable=True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("elt-articles-pipeline")
        # Spark 3.x dropped RFC-822 ("EEE, dd MMM yyyy HH:mm:ss Z") from the
        # new java.time parser. RSS pubDate is exactly that format, so we
        # opt back into the legacy SimpleDateFormat which still accepts it.
        # Scoped to this job; doesn't affect anything else.
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def extract(spark: SparkSession, source_path: str):
    """Permissive read; bad rows land in _corrupt_record."""
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(RAW_SCHEMA.add("_corrupt_record", StringType(), nullable=True))
        .csv(source_path)
    )


def transform(df):
    """
    Clean and cast. Quarantine rows that fail date/title validation.

    published_at can arrive in 3+ flavours depending on source:
      RSS pubDate (RFC-822):  "Thu, 05 Jun 2026 21:00:00 +0000"
      Atom updated  (ISO):    "2026-06-05T21:00:00Z"
      Open-data CSV:          "2026-06-05" or "2026" (year-only)
    We try each pattern in turn via coalesce; rows that match none get quarantined.
    """
    cleaned = (
        df
        .filter(F.col("title").isNotNull() & F.col("source").isNotNull()
                & F.col("_corrupt_record").isNull())
        .drop("_corrupt_record")

        .withColumn(
            "published_date",
            F.coalesce(
                F.to_date("published_at", "yyyy-MM-dd"),
                F.to_date("published_at", "yyyy-MM-dd'T'HH:mm:ssXXX"),
                F.to_date("published_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                F.to_date("published_at", "EEE, dd MMM yyyy HH:mm:ss Z"),
                F.to_date("published_at", "yyyy"),
            ),
        )

        # Trim + normalise short string fields.
        .withColumn("source",   F.lower(F.trim("source")))
        .withColumn("category", F.lower(F.trim("category")))
        .withColumn("title",    F.trim("title"))
        .withColumn("link",     F.trim("link"))

        # Strip HTML tags from summary so downstream BERT sees plain text.
        .withColumn("summary",
                    F.when(F.col("summary").isNotNull(),
                           F.regexp_replace("summary", r"<[^>]+>", " "))
                     .otherwise(F.lit(None))
                    )

        .withColumn("_pipeline_ts", F.current_timestamp())
        .withColumn("_source_path", F.input_file_name())
    )

    good = cleaned.filter(F.col("published_date").isNotNull())
    bad  = cleaned.filter(F.col("published_date").isNull())
    return good, bad


def load(df, dest_path: str, partition_col: str) -> int:
    (df.repartition(F.col(partition_col))
       .write.mode("overwrite")
       .partitionBy(partition_col)
       .parquet(dest_path))
    return df.count()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Articles ELT pipeline")
    parser.add_argument("--source-path",  default=None,
                        help="Fully-qualified raw input path; overrides --source-base/--date.")
    parser.add_argument("--source-base",  default=None,
                        help="Base raw prefix; YYYY/MM/DD/ from --date appended.")
    parser.add_argument("--date",         default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        help="Logical run date (UTC). Defaults to today.")
    parser.add_argument("--dest-path",    required=True)
    parser.add_argument("--partition-by", default="published_date")
    args = parser.parse_args(argv)

    if not args.source_path:
        if not args.source_base:
            parser.error("one of --source-path or --source-base is required")
        y, m, d = args.date.split("-")
        args.source_path = args.source_base.rstrip("/") + f"/{y}/{m}/{d}/"

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = extract(spark, args.source_path)
    raw_count = raw_df.count()
    print(f"[extract] rows read: {raw_count:,}")

    good_df, bad_df = transform(raw_df)
    good_count = load(good_df, args.dest_path, args.partition_by)
    print(f"[load] rows written (good): {good_count:,}")

    bad_count = bad_df.count()
    if bad_count:
        quarantine_path = args.dest_path.rstrip("/") + "_quarantine/"
        load(bad_df, quarantine_path, args.partition_by)
        print(f"[load] rows quarantined (bad date): {bad_count:,} → {quarantine_path}")

    reject_pct = (raw_count - good_count) / max(raw_count, 1) * 100
    print(f"[summary] reject rate: {reject_pct:.2f}%")

    # Articles pipelines tolerate higher reject rates than orders — feeds drop
    # malformed items frequently. Raise the gate to 20%.
    if reject_pct > 20.0:
        print(f"[ERROR] reject rate {reject_pct:.2f}% exceeds 20% threshold",
              file=sys.stderr)
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()
