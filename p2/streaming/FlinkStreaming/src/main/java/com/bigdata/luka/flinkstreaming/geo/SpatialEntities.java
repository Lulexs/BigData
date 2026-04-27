package com.bigdata.luka.flinkstreaming.geo;

import com.bigdata.luka.common.geo.LocationsList;

import java.util.ArrayList;
import java.util.List;

public class SpatialEntities {

    public static List<SpatialPoint> getPoints() {
        List<SpatialPoint> list = new ArrayList<>();

        for (int i = 0; i < LocationsList.POINTS.size(); i++) {
            double[] p = LocationsList.POINTS.get(i);
            list.add(new SpatialPoint(i, p[0], p[1]));
        }

        return list;
    }


    public static class SpatialPoint {
        public int entityId;
        public double lat;
        public double lon;

        public SpatialPoint(int entityId, double lat, double lon) {
            this.entityId = entityId;
            this.lat = lat;
            this.lon = lon;
        }
    }
}
