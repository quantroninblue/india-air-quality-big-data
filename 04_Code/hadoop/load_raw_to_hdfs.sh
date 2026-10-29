#!/usr/bin/env bash
# Copy the untouched raw CSVs from 02_Raw_Dataset into HDFS, then log the HDFS state.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PROJECT="$(cd "$HERE/../.." && pwd)"
source "$HERE/hadoop_env.sh"
cd "$PROJECT"
hdfs dfs -mkdir -p /cpcb/raw /cpcb/reference/stations_info /cpcb/lake /cpcb/mapreduce
hdfs dfs -put -f 02_Raw_Dataset/csv/*.csv /cpcb/raw/
hdfs dfs -cp -f /cpcb/raw/stations_info.csv /cpcb/reference/stations_info/
"$HERE/hdfs_report.sh"
