#!/usr/bin/env bash

docker exec -u 0 flink-jobmanager mkdir -p /opt/flink-apps

docker cp target/FlinkStreaming-assembly.jar \
  flink-jobmanager:/opt/flink-apps/FlinkStreaming.jar

docker exec flink-jobmanager /opt/flink/bin/flink run \
  -c com.bigdata.luka.flinkstreaming.FlinkStreamingApplication \
  /opt/flink-apps/FlinkStreaming.jar