package com.bigdata.luka.kafkaproducer.producers;

import com.bigdata.luka.kafkaproducer.model.KafkaEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaEventProducer {

    private final KafkaTemplate<String, KafkaEvent> kafkaTemplate;

    public void send(KafkaEvent event, String topic) {
        kafkaTemplate.send(topic, event.getKey(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish event for vehicleId={}, timestep={}",
                                event.getVehicleId(), event.getTimestamp(), ex);
                    } else {
                        log.info("Published event for vehicleId={}, timestep={}, topic={}, partition={}, offset={}",
                                event.getVehicleId(),
                                event.getTimestamp(),
                                topic,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}