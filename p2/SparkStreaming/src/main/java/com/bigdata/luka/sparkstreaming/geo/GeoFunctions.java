package com.bigdata.luka.sparkstreaming.geo;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.asin;
import static org.apache.spark.sql.functions.cos;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pow;
import static org.apache.spark.sql.functions.radians;
import static org.apache.spark.sql.functions.sin;
import static org.apache.spark.sql.functions.sqrt;

public final class GeoFunctions {

    private GeoFunctions() {
    }

    public static Column distance(
            Column lat1,
            Column lon1,
            Column lat2,
            Column lon2
    ) {
        Column dLat = radians(lat2.minus(lat1));
        Column dLon = radians(lon2.minus(lon1));

        Column a = pow(sin(dLat.divide(2.0)), 2.0)
                .plus(
                        cos(radians(lat1))
                                .multiply(cos(radians(lat2)))
                                .multiply(pow(sin(dLon.divide(2.0)), 2.0))
                );

        Column c = asin(sqrt(a)).multiply(2.0);

        return c.multiply(lit(6371000.0));
    }
}