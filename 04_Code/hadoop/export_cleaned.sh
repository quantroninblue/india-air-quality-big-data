#!/usr/bin/env bash
# Copy the cleaned layers from HDFS into 03_Cleaned_Dataset (hdfs dfs -get).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PROJECT="$(cd "$HERE/../.." && pwd)"
source "$HERE/hadoop_env.sh"
DEST="$PROJECT/03_Cleaned_Dataset"
for layer in silver_stations silver_hourly gold_daily; do
  rm -rf "${DEST:?}/$layer"
  hdfs dfs -get "/cpcb/lake/$layer" "$DEST/"
done
find "$DEST" -name "*.crc" -delete
du -sh "$DEST"/*
