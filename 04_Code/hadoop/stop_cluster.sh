#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/hadoop_env.sh"
yarn --daemon stop nodemanager
yarn --daemon stop resourcemanager
hdfs --daemon stop secondarynamenode
hdfs --daemon stop datanode
hdfs --daemon stop namenode
