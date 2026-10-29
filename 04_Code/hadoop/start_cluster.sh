#!/usr/bin/env bash
# Start HDFS (NameNode, DataNode, SecondaryNameNode) and YARN (ResourceManager,
# NodeManager) as background daemons. Uses --daemon, so no SSH server is needed.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/hadoop_env.sh"
hdfs --daemon start namenode
hdfs --daemon start datanode
hdfs --daemon start secondarynamenode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
hdfs dfsadmin -safemode wait
"$HADOOP_JAVA_HOME/bin/jps" | sort -k2
echo "NameNode UI: http://localhost:9870   YARN UI: http://localhost:8088"
