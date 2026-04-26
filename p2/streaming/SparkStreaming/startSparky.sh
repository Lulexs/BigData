#!/usr/bin/env bash
set -e

docker exec -u 0 spark-master mkdir -p /opt/spark-apps
docker cp target/SparkStreaming-assembly.jar spark-master:/opt/spark-apps/SparkStreaming-assembly.jar

docker exec spark-master /opt/spark/bin/spark-submit \
  --class com.bigdata.luka.sparkstreaming.SparkStreamingApplication \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  /opt/spark-apps/SparkStreaming-assembly.jar