package com.bigdata.luka.flinkstreaming.model;

public class TrafficResult {
    public long windowStart;
    public long windowEnd;
    public int entityId;
    public double entityLat;
    public double entityLon;
    public long vehicleCount;
    public double radiusMeters;
    public String analysisType;

    public TrafficResult() {}
}
