"""
Stage 1 – Distributed article scraping: RSS (XML) feeds + CSV → S3 landing zone.

Each executor independently fetches a slice of URLs and writes the raw
content as normalised CSV rows to S3. The driver only coordinates; all
HTTP work happens on executors.

NOTE on filename: kept as scrape_articles.py for ArgoCD / SparkJob path stability,
even though the demo now scrapes tech-news articles instead of orders. The
"orders" schema was a generic skeleton.

URL manifest (scrape_urls.json) format:
  [
    {"url": "https://hnrss.org/frontpage",            "format": "xml", "source": "hn_frontpage"},
    {"url": "https://www.reddit.com/r/programming.rss","format": "xml", "source": "reddit_prog"},
    {"url": "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv",
     "format": "csv", "source": "owid_co2"}
  ]

Output schema (all values stringified — typing happens in ELT):
  id, source, title, link, published_at, summary, category,
  _source, _scraped_at

Usage:
  scrape_articles.py \
    --url-manifest s3a://bucket/config/scrape_urls.json \
    --dest-base    s3a://bucket/raw/articles/           # YYYY/MM/DD appended
    [--date 2026-06-05]                                  # defaults to today UTC
"""

import argparse
import csv
import hashlib
import io
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Iterator
from urllib.request import Request, urlopen
from urllib.error import URLError

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


OUT_SCHEMA = StructType([
    StructField("id",           StringType(), True),
    StructField("source",       StringType(), True),
    StructField("title",        StringType(), True),
    StructField("link",         StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("summary",      StringType(), True),
    StructField("category",     StringType(), True),
    StructField("_source",      StringType(), True),
    StructField("_scraped_at",  StringType(), True),
])

_TIMEOUT = 30

# Reddit & many CDNs reject the default urllib UA; spoof a browser.
_UA = "Mozilla/5.0 (compatible; spark-scraper/1.0; +https://github.com/huzaifa678/compute-operator)"

# RSS namespaces we strip to make tag matching trivial.
_NS_RE = re.compile(r"^\{[^}]+\}")


def _fetch(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": _UA, "Accept": "*/*"})
    with urlopen(req, timeout=_TIMEOUT) as resp:
        return resp.read()


def _stable_id(*parts: str) -> str:
    """Deterministic ID from (link, title) so dedup works across runs."""
    h = hashlib.sha1("\x1f".join(p or "" for p in parts).encode("utf-8")).hexdigest()
    return h[:16]


def _strip_ns(tag: str) -> str:
    return _NS_RE.sub("", tag).lower()


def _parse_rss(raw: bytes, source: str, scraped_at: str) -> Iterator[dict]:
    """Parse RSS 2.0 / Atom feeds. Walks every <item> or <entry> element."""
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        print(f"[scrape] WARN bad XML from {source}: {exc}", flush=True)
        return

    for elem in root.iter():
        tag = _strip_ns(elem.tag)
        if tag not in ("item", "entry"):
            continue
        rec: dict = {}
        for child in elem:
            ctag = _strip_ns(child.tag)
            # Atom <link href=…/>, RSS <link>https://…</link>
            if ctag == "link":
                rec["link"] = child.attrib.get("href") or (child.text or "").strip()
            elif ctag in ("title", "description", "summary", "content"):
                # Map RSS<description>/Atom<summary>|<content> to summary.
                if ctag == "title":
                    rec["title"] = (child.text or "").strip()
                else:
                    rec.setdefault("summary", (child.text or "").strip())
            elif ctag in ("pubdate", "published", "updated"):
                rec.setdefault("published_at", (child.text or "").strip())
            elif ctag == "category":
                rec.setdefault(
                    "category",
                    child.attrib.get("term") or (child.text or "").strip(),
                )
            elif ctag in ("guid", "id"):
                rec.setdefault("id", (child.text or "").strip())
        if not rec.get("title") and not rec.get("link"):
            continue
        rec.setdefault("id", _stable_id(rec.get("link", ""), rec.get("title", "")))
        rec["_source"] = source
        rec["_scraped_at"] = scraped_at
        yield rec


def _parse_csv(raw: bytes, source: str, scraped_at: str) -> Iterator[dict]:
    """Pass-through CSV. ELT does the heavy normalisation."""
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8", errors="replace")))
    for row in reader:
        # Lower-case + trim keys; coalesce empty strings to None.
        norm = {(k or "").strip().lower(): (v.strip() if isinstance(v, str) else v) or None
                for k, v in row.items()}
        # Many open-data CSVs (e.g. OWID) have no notion of title/link — synthesise.
        title = norm.get("title") or norm.get("country") or norm.get("name")
        published_at = norm.get("published_at") or norm.get("date") or norm.get("year")
        norm.setdefault("title", title)
        norm.setdefault("published_at", str(published_at) if published_at else None)
        norm.setdefault("link", norm.get("link") or norm.get("url"))
        norm.setdefault("summary", norm.get("summary") or norm.get("description"))
        norm.setdefault("category", norm.get("category") or norm.get("type"))
        norm.setdefault("id", _stable_id(norm.get("link") or "", norm.get("title") or ""))
        norm["_source"] = source
        norm["_scraped_at"] = scraped_at
        yield norm


def scrape_partition(entries: Iterator[Row]) -> Iterator[Row]:
    """mapPartitions: each executor handles its slice of URL entries."""
    scraped_at = datetime.now(timezone.utc).isoformat()
    for entry in entries:
        url = entry["url"]
        fmt = (entry["format"] or "").lower()
        source = entry["source"] or url
        try:
            raw = _fetch(url)
        except (URLError, OSError) as exc:
            print(f"[scrape] WARN failed to fetch {url}: {exc}", flush=True)
            continue
        # Be polite: small inter-URL pause per executor to avoid hammering CDNs.
        time.sleep(0.5)

        parser = _parse_rss if fmt == "xml" else _parse_csv
        for rec in parser(raw, source, scraped_at):
            yield Row(
                id           = rec.get("id"),
                source       = source,
                title        = rec.get("title"),
                link         = rec.get("link"),
                published_at = rec.get("published_at"),
                summary      = rec.get("summary"),
                category     = rec.get("category"),
                _source      = source,
                _scraped_at  = scraped_at,
            )


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-manifest", required=True)
    parser.add_argument("--dest-path",    default=None)
    parser.add_argument("--dest-base",    default=None,
                        help="Base S3 prefix; YYYY/MM/DD/ appended from --date.")
    parser.add_argument("--date",         default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        help="Logical run date (UTC). Defaults to today.")
    args = parser.parse_args(argv)

    if not args.dest_path and not args.dest_base:
        parser.error("one of --dest-path or --dest-base is required")
    if not args.dest_path:
        y, m, d = args.date.split("-")
        args.dest_path = args.dest_base.rstrip("/") + f"/{y}/{m}/{d}/"

    spark = SparkSession.builder.appName("scrape-articles-raw").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    manifest_rdd = sc.textFile(args.url_manifest)
    entries = json.loads("\n".join(manifest_rdd.collect()))
    if not entries:
        print("[scrape] ERROR: empty URL manifest", file=sys.stderr); sys.exit(1)

    urls_df = spark.createDataFrame(entries)
    num_parts = min(len(entries), int(spark.conf.get("spark.executor.instances", "4")))
    scraped_rdd = urls_df.rdd.repartition(num_parts).mapPartitions(scrape_partition)
    result_df = spark.createDataFrame(scraped_rdd, schema=OUT_SCHEMA) \
        .withColumn("scrape_date", F.lit(args.date))

    count = result_df.count()
    print(f"[scrape] total rows scraped: {count:,}")
    if count == 0:
        print("[scrape] ERROR: no rows scraped from any source", file=sys.stderr); sys.exit(1)

    (result_df
        .repartition(F.col("_source"))
        .write.mode("overwrite")
        .partitionBy("scrape_date", "_source")
        .option("header", "true")
        .csv(args.dest_path))
    print(f"[scrape] written to {args.dest_path}")
    spark.stop()


if __name__ == "__main__":
    main()
