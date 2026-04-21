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
public class VehicleEmissionEvent {

    private BigDecimal timestep;

    private String id;
    private String eclass;

    private BigDecimal co2;
    private BigDecimal co;
    private BigDecimal hc;
    private BigDecimal nox;
    private BigDecimal pmx;
    private BigDecimal fuel;
    private BigDecimal electricity;
    private BigDecimal noise;

    private String route;
    private String type;

    private BigDecimal waiting;
    private String lane;
    private BigDecimal pos;
    private BigDecimal speed;
    private BigDecimal angle;
    private BigDecimal x;
    private BigDecimal y;
}