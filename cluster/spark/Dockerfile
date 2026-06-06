# Spark base image with S3A support baked in.
#
# apache/spark:3.5.3 ships Hadoop 3.3.4 libs but DOES NOT include hadoop-aws
# or the AWS SDK bundle, so any sparkConf using s3a:// dies with:
#   java.lang.ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem
#
# The jars are fetched by `make spark-jars` into ./jars/ (next to this file),
# not by `ADD <url>` — that gave us 600s docker builds with no resume on a
# 280 MB jar. Local download is curl-resumable and cached across rebuilds.
#
# Version matrix (do NOT mix-and-match — Hadoop pins to an exact AWS SDK):
#   spark      3.5.3
#   hadoop     3.3.4   (bundled with Spark)
#   hadoop-aws 3.3.4
#   aws-sdk    1.12.262
#
# Build:
#   make spark-image         # downloads jars if missing, then builds
#   make spark-image-k3d     # also imports into the k3d cluster
FROM apache/spark:3.5.3

USER root

COPY --chown=spark:spark jars/hadoop-aws-3.3.4.jar           /opt/spark/jars/
COPY --chown=spark:spark jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

RUN chmod 0644 /opt/spark/jars/hadoop-aws-3.3.4.jar \
               /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER spark
