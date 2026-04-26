package com.bigdata.luka.sparkstreaming.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.functions.*;

public final class KafkaResultSink {

    private KafkaResultSink() {
    }

    public static StreamingQuery write(
            Dataset<Row> analysisDf,
            String bootstrapServers,
            String outputTopic,
            String checkpointLocation,
            String triggerInterval,
            String analysisName
    ) throws Exception {

        Dataset<Row> kafkaOut = analysisDf
                .select(
                        col("analysisType").alias("key"),
                        to_json(struct(col("*"))).alias("value")
                );

        return kafkaOut.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("topic", outputTopic)
                .option("checkpointLocation", checkpointLocation + "/" + analysisName)
                .trigger(Trigger.ProcessingTime(triggerInterval))
                .outputMode("update")
                .start();
    }
}