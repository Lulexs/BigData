package com.bigdata.luka.sparkstreaming.analysis;

import com.bigdata.luka.common.config.WindowConfig;
import com.bigdata.luka.common.geo.LocationsList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.asin;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.cos;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pow;
import static org.apache.spark.sql.functions.radians;
import static org.apache.spark.sql.functions.sin;
import static org.apache.spark.sql.functions.sqrt;
import static org.apache.spark.sql.functions.window;

public interface Analysis {
    double RADIUS = 200;

    String getAnalysisName();

    Dataset<Row> analyze(SparkSession session, Dataset<Row> inputStream, WindowConfig windowConfig);

    // distance between 2 points: https://en.wikipedia.org/wiki/Haversine_formula
    static Column haversineMeters(Column lat1, Column lon1, Column lat2, Column lon2) {
        double earthRadiusMeters = 6371000.0;

        Column dLat = radians(lat2.minus(lat1));
        Column dLon = radians(lon2.minus(lon1));

        Column a = pow(sin(dLat.divide(2)), 2)
                .plus(
                        cos(radians(lat1))
                                .multiply(cos(radians(lat2)))
                                .multiply(pow(sin(dLon.divide(2)), 2))
                );

        return lit(earthRadiusMeters).multiply(lit(2)).multiply(asin(sqrt(a)));
    }

    default Dataset<Row> locationsDataset(SparkSession sparkSession) {
        List<Row> rows = LocationsList.POINTS.stream()
                .map(p -> RowFactory.create(p[0], p[1]))
                .collect(Collectors.toList());

        StructType schema = new StructType()
                .add("location_x", DataTypes.DoubleType, false)
                .add("location_y", DataTypes.DoubleType, false);

        return sparkSession.createDataFrame(rows, schema);
    }

    default Column buildWindowColumn(WindowConfig windowConfig) {
        if (windowConfig.isTumbling()) {
            return window(
                    col("event_time"),
                    windowConfig.getLength()
            );
        }

        if (windowConfig.isSliding()) {
            return window(
                    col("event_time"),
                    windowConfig.getLength(),
                    windowConfig.getSlide()
            );
        }

        throw new IllegalArgumentException(
                "Unsupported window type: " + windowConfig.getType()
        );
    }

}
