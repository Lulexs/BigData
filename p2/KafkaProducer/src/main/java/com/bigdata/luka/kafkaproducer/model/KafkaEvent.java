package com.bigdata.luka.kafkaproducer.model;

public interface KafkaEvent {

    String getKey();

    String getVehicleId();

    Long getTimestamp();

}
