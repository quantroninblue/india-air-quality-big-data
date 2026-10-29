#!/usr/bin/env bash
# Generic options (-D, -files) must come before streaming options. ignoreKey drops the
# byte-offset key that CombineTextInputFormat would otherwise prepend to every line.
# Hadoop MapReduce (Streaming, on YARN): hourly readings and raw mean PM2.5 per year,
# computed directly from the raw CSVs in HDFS. Output copied to 05_Outputs/tables.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PROJECT="$(cd "$HERE/../.." && pwd)"
source "$HERE/hadoop_env.sh"
OUT=/cpcb/mapreduce/pm25_by_year
LOG="$PROJECT/05_Outputs/logs/mapreduce_job_log.txt"
STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar)

hdfs dfs -rm -r -f -skipTrash "$OUT" >/dev/null
# local sanity test of the scripts (the classic "cat | map | sort | reduce" check)
hdfs dfs -cat /cpcb/raw/DL001.csv | python3 "$HERE/mapreduce/mapper.py" | sort | python3 "$HERE/mapreduce/reducer.py" | head -3

start=$(date +%s)
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="cpcb-pm25-by-year" \
  -D mapreduce.input.fileinputformat.split.maxsize=268435456 \
  -D mapreduce.job.reduces=2 \
  -D stream.map.input.ignoreKey=true \
  -files "$HERE/mapreduce/mapper.py,$HERE/mapreduce/reducer.py" \
  -inputformat org.apache.hadoop.mapred.lib.CombineTextInputFormat \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input "/cpcb/raw/[A-Z]*.csv" -output "$OUT" > "$LOG" 2>&1
echo "wall_seconds=$(( $(date +%s) - start ))" >> "$LOG"

hdfs dfs -ls "$OUT"
{ echo "year,hourly_rows,pm25_valid,mean_pm25_raw"; hdfs dfs -cat "$OUT/part-*" | sort | tr '\t' ','; } \
  > "$PROJECT/05_Outputs/tables/mapreduce_pm25_by_year.csv"
grep -E "Running job|Job .* completed|Launched map tasks|Launched reduce tasks|Map input records|Map output records|Reduce input groups|Reduce output records|Shuffled Maps|Bytes Read=|Bytes Written=|wall_seconds" "$LOG"
