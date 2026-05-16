package com.bigdata.luka.flinkstreaming;

import java.io.Serializable;

public class KafkaOutputEvent implements Serializable {

    public String key;
    public String value;

    public KafkaOutputEvent() {
    }

    public KafkaOutputEvent(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
