#!/usr/bin/env bash
# Upload PySpark job sources + scrape manifest to S3 in the layout the
# SparkJob CRs reference. One command, no path-typo surprises.
#
# Layout (matches config/samples/*.yaml):
#   s3://$BUCKET/jobs/scrape_articles.py
#   s3://$BUCKET/jobs/elt_articles.py
#   s3://$BUCKET/config/scrape_urls.json
#
# Usage:
#   BUCKET=spark-lake-2998043 ./scripts/upload-jobs.sh
#   ./scripts/upload-jobs.sh --bucket spark-lake-2998043 --dry-run
#   ./scripts/upload-jobs.sh --bucket spark-lake-2998043 --region us-east-1
#
# Auth: standard AWS SDK chain — env vars, ~/.aws/credentials, IRSA, etc.
set -euo pipefail

BUCKET="${BUCKET:-}"
REGION_FLAG=""
DRY_RUN=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket)   BUCKET="$2"; shift 2 ;;
    --region)   REGION_FLAG="--region $2"; shift 2 ;;
    --dry-run)  DRY_RUN="--dryrun"; shift ;;
    -h|--help)
      sed -n '2,15p' "$0"; exit 0 ;;
    *)  echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$BUCKET" ]]; then
  echo "ERROR: set BUCKET=... or pass --bucket <name>" >&2
  exit 2
fi
if ! command -v aws >/dev/null; then
  echo "ERROR: aws CLI not on PATH (install with: brew install awscli)" >&2
  exit 2
fi

# Resolve repo root (this script lives in scripts/).
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Each line: "<local-path> -> <s3-key>"
TARGETS=(
  "$ROOT/jobs/scrape_articles.py:jobs/scrape_articles.py"
  "$ROOT/jobs/elt_articles.py:jobs/elt_articles.py"
  "$ROOT/jobs/scrape_urls.json:config/scrape_urls.json"
)

echo "[upload-jobs] bucket=s3://$BUCKET ${DRY_RUN:+(dry-run)}"
for spec in "${TARGETS[@]}"; do
  src="${spec%%:*}"; dst_key="${spec#*:}"
  if [[ ! -s "$src" ]]; then
    echo "  SKIP missing $src" >&2; continue
  fi
  # Per-file content-type so S3 / browsers see a sensible MIME.
  case "$dst_key" in
    *.py)    ct="text/x-python" ;;
    *.json)  ct="application/json" ;;
    *)       ct="application/octet-stream" ;;
  esac
  echo "  PUT  $src  ->  s3://$BUCKET/$dst_key  ($ct)"
  aws s3 cp $DRY_RUN $REGION_FLAG \
      --content-type "$ct" \
      --no-progress \
      "$src" "s3://$BUCKET/$dst_key"
done
echo "[upload-jobs] done"
