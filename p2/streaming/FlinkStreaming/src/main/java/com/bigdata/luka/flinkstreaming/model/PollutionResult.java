package com.bigdata.luka.flinkstreaming.model;

public class PollutionResult {
    public long windowStart;
    public long windowEnd;
    public int entityId;
    public double entityLat;
    public double entityLon;
    public double avgCo2;
    public double avgCo;
    public double avgHc;
    public double avgNox;
    public double avgPmx;
    public double avgNoise;
    public long eventCount;
    public double radiusMeters;
    public String analysisType;

    public PollutionResult() {
    }
}
