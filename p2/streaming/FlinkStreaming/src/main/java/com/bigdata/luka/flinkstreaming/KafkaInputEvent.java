package com.bigdata.luka.flinkstreaming;

public class KafkaInputEvent {

    public String topic;
    public String value;

    public KafkaInputEvent() {
    }

    public KafkaInputEvent(String topic, String value) {
        this.topic = topic;
        this.value = value;
    }
}
