package com.bigdata.luka.flinkstreaming.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class KafkaResultSink {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();

    private KafkaResultSink() {
    }

    public static <T> KafkaSink<T> create(
            String bootstrapServers,
            String topic,
            String analysisName
    ) {

        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer((KafkaRecordSerializationSchema<T>) (element, context, timestamp) -> {
                    try {
                        String key = analysisName;
                        String value = MAPPER.writeValueAsString(element);

                        return new ProducerRecord<>(
                                topic,
                                STRING_SCHEMA.serialize(key),
                                STRING_SCHEMA.serialize(value)
                        );

                    } catch (Exception e) {
                        throw new RuntimeException("Serialization failed", e);
                    }
                })
                .build();
    }
}
