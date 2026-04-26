package com.bigdata.luka.kafkaproducer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VehicleFcdEvent implements KafkaEvent {

    private BigDecimal timestep;

    private String id;
    private BigDecimal x;
    private BigDecimal y;
    private BigDecimal angle;
    private String type;
    private BigDecimal speed;
    private BigDecimal pos;
    private String lane;
    private BigDecimal slope;

    @Override
    public String getKey() {
        return id;
    }

    @Override
    public String getVehicleId() {
        return id;
    }

    @Override
    public BigDecimal getTimestamp() {
        return timestep;
    }
}