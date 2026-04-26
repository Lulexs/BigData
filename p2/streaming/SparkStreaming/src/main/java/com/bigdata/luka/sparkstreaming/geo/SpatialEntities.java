package com.bigdata.luka.sparkstreaming.geo;


import com.bigdata.luka.common.geo.LocationsList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public final class SpatialEntities {

    private SpatialEntities() {
    }

    public static Dataset<Row> asDataFrame(SparkSession spark) {
        List<SpatialPoint> rows = new ArrayList<>();

        for (int i = 0; i < LocationsList.POINTS.size(); i++) {
            double[] point = LocationsList.POINTS.get(i);
            rows.add(new SpatialPoint(i, point[0], point[1]));
        }

        return spark.createDataFrame(rows, SpatialPoint.class);
    }

    public static class SpatialPoint implements Serializable {
        private int entityId;
        private double lat;
        private double lon;

        public SpatialPoint() {
        }

        public SpatialPoint(int entityId, double lat, double lon) {
            this.entityId = entityId;
            this.lat = lat;
            this.lon = lon;
        }

        public int getEntityId() {
            return entityId;
        }

        public void setEntityId(int entityId) {
            this.entityId = entityId;
        }

        public double getLat() {
            return lat;
        }

        public void setLat(double lat) {
            this.lat = lat;
        }

        public double getLon() {
            return lon;
        }

        public void setLon(double lon) {
            this.lon = lon;
        }
    }
}