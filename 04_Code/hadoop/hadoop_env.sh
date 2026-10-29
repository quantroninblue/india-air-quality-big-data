# Source this file to use the project's single-node Hadoop cluster from a shell:
#   source 04_Code/hadoop/hadoop_env.sh
# JAVA_HOME is NOT exported here (Spark 4 needs Java 17+); Hadoop's own scripts read
# Java 11 from hadoop-env.sh inside the generated conf directory.
export HADOOP_HOME="${HADOOP_HOME:-$HOME/opt/hadoop-3.4.1}"
export HADOOP_JAVA_HOME="${HADOOP_JAVA_HOME:-$HOME/opt/jdk11}"
export HADOOP_DATA="${HADOOP_DATA:-$HOME/opt/hadoop_data}"
export HADOOP_CONF_DIR="$HADOOP_DATA/conf"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HADOOP_JAVA_HOME/bin:$PATH"
unset PYTHONPATH
