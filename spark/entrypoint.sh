#!/bin/bash
set -e

envsubst < "${SPARK_HOME}/conf/spark-defaults.conf.template" \
         > "${SPARK_HOME}/conf/spark-defaults.conf"

echo "Prefetching Spark packages..."
"${SPARK_HOME}/bin/spark-submit" \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\
org.apache.spark:spark-avro_2.12:3.4.1 \
  --class org.apache.spark.examples.SparkPi \
  --master local[*] \
  "${SPARK_HOME}/examples/jars/spark-examples_2.12-3.4.1.jar" 1

cp /tmp/.ivy2/jars/*.jar "${SPARK_HOME}/jars/"

"${SPARK_HOME}/sbin/start-thriftserver.sh" --master local[*]
tail -f /dev/null
