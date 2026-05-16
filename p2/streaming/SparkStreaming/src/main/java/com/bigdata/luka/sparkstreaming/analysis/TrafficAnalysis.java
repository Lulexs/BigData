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

import static org.apache.spark.sql.functions.approx_count_distinct;
import static org.apache.spark.sql.functions.asin;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.cos;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pow;
import static org.apache.spark.sql.functions.radians;
import static org.apache.spark.sql.functions.sin;
import static org.apache.spark.sql.functions.sqrt;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.window;

public class TrafficAnalysis implements Analysis {

    private static final String NAME = "traffic";
    private static final double RADIUS = 200;

    @Override
    public String getAnalysisName() {
        return NAME;
    }

    @Override
    public Dataset<Row> analyze(SparkSession sparkSession, Dataset<Row> inputStream, WindowConfig windowConfig) {
        StructType fcdSchema = new StructType()
                .add("timestep", DataTypes.LongType)
                .add("id", DataTypes.StringType)
                .add("x", DataTypes.DoubleType)
                .add("y", DataTypes.DoubleType)
                .add("angle", DataTypes.DoubleType)
                .add("type", DataTypes.StringType)
                .add("speed", DataTypes.DoubleType)
                .add("pos", DataTypes.DoubleType)
                .add("lane", DataTypes.StringType)
                .add("slope", DataTypes.DoubleType)
                .add("key", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType)
                .add("vehicleId", DataTypes.StringType);

        Dataset<Row> fcd = inputStream
                .filter("topic = 'FCD'")
                .select(
                        from_json(col("value").cast("string"), fcdSchema).alias("data")
                )
                .select(
                        col("data.vehicleId").alias("id"),
                        col("data.y").alias("vehicle_lat"),
                        col("data.x").alias("vehicle_lon"),
                        to_timestamp(
                                from_unixtime(col("data.timestamp").divide(1000))
                        ).alias("event_time")
                );

        Column windowColumn = buildWindowColumn(windowConfig);

        Dataset<Row> result = fcd
                .withWatermark("event_time", windowConfig.getWatermarkDelay())
                .crossJoin(locationsDataset(sparkSession))
                .filter(
                        haversineMeters(
                                col("vehicle_lat"),
                                col("vehicle_lon"),
                                col("location_x"),
                                col("location_y")
                        ).leq(RADIUS)
                )
                .groupBy(
                        windowColumn.alias("window"),
                        col("location_x"),
                        col("location_y")
                )
                .agg(
                        approx_count_distinct("id").alias("distinct_cars_count")
                );

        return result.select(
                concat_ws(
                        "/",
                        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss"),
                        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss"),
                        col("location_x").cast("string"),
                        col("location_y").cast("string")
                ).alias("key"),
                to_json(
                        struct(
                                col("window.start").alias("window_start"),
                                col("window.end").alias("window_end"),
                                col("location_x"),
                                col("location_y"),
                                col("distinct_cars_count")
                        )
                ).alias("value")
        );
    }

    private Column buildWindowColumn(WindowConfig windowConfig) {
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

    private Dataset<Row> locationsDataset(SparkSession sparkSession) {
        List<Row> rows = LocationsList.POINTS.stream()
                .map(p -> RowFactory.create(p[0], p[1]))
                .collect(Collectors.toList());

        StructType schema = new StructType()
                .add("location_x", DataTypes.DoubleType, false)
                .add("location_y", DataTypes.DoubleType, false);

        return sparkSession.createDataFrame(rows, schema);
    }

    // distance between 2 points: https://en.wikipedia.org/wiki/Haversine_formula
    private static Column haversineMeters(
            Column lat1,
            Column lon1,
            Column lat2,
            Column lon2
    ) {
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
}
