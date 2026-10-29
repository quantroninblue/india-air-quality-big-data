#!/usr/bin/env bash
# One-time setup: build the runtime conf dir from the Hadoop defaults + this project's
# XML files, then format the NameNode (only if it has never been formatted).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/hadoop_env.sh"

mkdir -p "$HADOOP_CONF_DIR" "$HADOOP_DATA"/{logs,pids,tmp}
cp -r "$HADOOP_HOME/etc/hadoop/." "$HADOOP_CONF_DIR/"
cp "$HERE"/conf/*.xml "$HADOOP_CONF_DIR/"
cat >> "$HADOOP_CONF_DIR/hadoop-env.sh" <<ENV

# --- CPCB project ---
export JAVA_HOME="$HADOOP_JAVA_HOME"
export HADOOP_HOME="$HADOOP_HOME"
export HADOOP_LOG_DIR="$HADOOP_DATA/logs"
export HADOOP_PID_DIR="$HADOOP_DATA/pids"
ENV
cat >> "$HADOOP_CONF_DIR/yarn-env.sh" <<ENV
export JAVA_HOME="$HADOOP_JAVA_HOME"
ENV

if [ ! -d "$HADOOP_DATA/namenode/current" ]; then
  hdfs namenode -format -nonInteractive -clusterId cpcb-cluster
fi
echo "Hadoop conf ready in $HADOOP_CONF_DIR"
