package com.bigdata.luka.kafkaproducer.model;

import java.math.BigDecimal;

public interface KafkaEvent {

    String getKey();

    String getVehicleId();

    BigDecimal getTimestamp();

}
