package com.bigdata.luka.flinkstreaming.parser;

public class FcdEvent {
    public String sourceTopic;
    public long eventTime;

    public Double timestep;
    public Double timestamp;

    public String id;
    public String vehicleId;
    public String key;

    public String vehicleType;
    public Double angle, speed, pos, slope;

    public String lane;

    public Double lat;
    public Double lon;
}
