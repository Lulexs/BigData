package com.bigdata.luka.flinkstreaming.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public final class KafkaResultSink {

    private static final ObjectMapper MAPPER = new ObjectMapper();

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
                        byte[] value = MAPPER.writeValueAsBytes(element);
                        byte[] key = analysisName.getBytes(StandardCharsets.UTF_8);

                        return new ProducerRecord<>(topic, key, value);

                    } catch (Exception e) {
                        throw new RuntimeException("Serialization failed", e);
                    }
                })
                .build();
    }
}
