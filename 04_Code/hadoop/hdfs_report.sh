#!/usr/bin/env bash
# Record common HDFS shell commands and their output in 05_Outputs/logs/hdfs_shell_log.txt.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PROJECT="$(cd "$HERE/../.." && pwd)"
source "$HERE/hadoop_env.sh"
LOG="$PROJECT/05_Outputs/logs/hdfs_shell_log.txt"
: > "$LOG"
run() { echo "\$ $1" >> "$LOG"; bash -c "$1" >> "$LOG" 2>&1; echo >> "$LOG"; }

run "jps | sort -k2"
run "hdfs version | head -1"
run "hdfs dfs -ls /cpcb"
run "hdfs dfs -ls /cpcb/raw | head -6"
run "hdfs dfs -count -h /cpcb/raw"
run "hdfs dfs -du -s -h /cpcb/raw /cpcb/lake"
run "hdfs dfs -head /cpcb/raw/DL001.csv | head -3"
run "hdfs dfs -stat 'name=%n size=%b replication=%r blocksize=%o' /cpcb/raw/DL001.csv"
run "hdfs fsck /cpcb/raw | grep -E 'Total size|Total files|Total blocks|Average block|Default replication|Corrupt blocks|Missing blocks:|HEALTHY' | head -9"
run "hdfs dfsadmin -report | grep -E 'Configured Capacity|DFS Used:|Live datanodes|Name:'"
run "yarn node -list 2>/dev/null"
echo "logged to $LOG"
