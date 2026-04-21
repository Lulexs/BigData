package com.bigdata.luka.kafkaproducer;

import com.bigdata.luka.kafkaproducer.model.VehicleEmissionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmissionKafkaProducer {

    private final KafkaTemplate<String, VehicleEmissionEvent> kafkaTemplate;

    private static final String TOPIC = "EMISSIONS";

    public void send(VehicleEmissionEvent event) {
        kafkaTemplate.send(TOPIC, event.getId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish event for vehicleId={}, timestep={}",
                                event.getId(), event.getTimestep(), ex);
                    } else {
                        log.info("Published event for vehicleId={}, timestep={}, topic={}, partition={}, offset={}",
                                event.getId(),
                                event.getTimestep(),
                                TOPIC,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}