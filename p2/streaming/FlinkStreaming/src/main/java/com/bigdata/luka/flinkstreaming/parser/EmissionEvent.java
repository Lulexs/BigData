package com.bigdata.luka.flinkstreaming.parser;

public class EmissionEvent {
    public String sourceTopic;
    public long eventTime;

    public Double timestep;
    public Double timestamp;

    public String id;
    public String vehicleId;
    public String key;

    public String eclass;
    public String vehicleType;

    public Double co2, co, hc, nox, pmx, fuel, electricity, noise;

    public String route, lane;
    public Double waiting, pos, speed, angle;

    public Double x, y;
}
