package com.bigdata.luka.flinkstreaming.model;

public class Match {
    public String vehicleId;
    public long eventTime;
    public int entityId;
    public double lat;
    public double lon;

    public Match() {}

    public Match(String vehicleId, long eventTime,
                 int entityId, double lat, double lon) {
        this.vehicleId = vehicleId;
        this.eventTime = eventTime;
        this.entityId = entityId;
        this.lat = lat;
        this.lon = lon;
    }
}
