package com.bigdata.luka.flinkstreaming;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class KafkaInputEventDeserializer implements KafkaRecordDeserializationSchema<KafkaInputEvent> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaInputEvent> out) throws IOException {
        String value = record.value() == null
                ? null
                : new String(record.value(), StandardCharsets.UTF_8);

        out.collect(new KafkaInputEvent(record.topic(), value));
    }

    @Override
    public TypeInformation<KafkaInputEvent> getProducedType() {
        return TypeInformation.of(KafkaInputEvent.class);
    }
}
