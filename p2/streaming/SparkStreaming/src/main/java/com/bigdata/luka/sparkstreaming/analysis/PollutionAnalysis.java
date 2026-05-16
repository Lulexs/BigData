package com.bigdata.luka.sparkstreaming.analysis;

import com.bigdata.luka.common.config.WindowConfig;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.to_timestamp;

public class PollutionAnalysis implements Analysis {

    private static final String NAME = "pollution";

    @Override
    public String getAnalysisName() {
        return NAME;
    }

    @Override
    public Dataset<Row> analyze(
            SparkSession sparkSession,
            Dataset<Row> inputStream,
            WindowConfig windowConfig
    ) {
        StructType emissionSchema = new StructType()
                .add("timestep", DataTypes.LongType)
                .add("id", DataTypes.StringType)
                .add("eclass", DataTypes.StringType)
                .add("co2", DataTypes.DoubleType)
                .add("co", DataTypes.DoubleType)
                .add("hc", DataTypes.DoubleType)
                .add("nox", DataTypes.DoubleType)
                .add("pmx", DataTypes.DoubleType)
                .add("fuel", DataTypes.DoubleType)
                .add("electricity", DataTypes.DoubleType)
                .add("noise", DataTypes.DoubleType)
                .add("route", DataTypes.StringType)
                .add("type", DataTypes.StringType)
                .add("waiting", DataTypes.DoubleType)
                .add("lane", DataTypes.StringType)
                .add("pos", DataTypes.DoubleType)
                .add("speed", DataTypes.DoubleType)
                .add("angle", DataTypes.DoubleType)
                .add("x", DataTypes.DoubleType)
                .add("y", DataTypes.DoubleType)
                .add("key", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType)
                .add("vehicleId", DataTypes.StringType);

        Dataset<Row> emissions = inputStream
                .filter("topic = 'EMISSIONS'")
                .select(
                        from_json(col("value").cast("string"), emissionSchema).alias("data")
                )
                .select(
                        col("data.vehicleId").alias("vehicle_id"),
                        col("data.y").alias("vehicle_lat"),
                        col("data.x").alias("vehicle_lon"),
                        col("data.co2").alias("co2"),
                        col("data.co").alias("co"),
                        col("data.hc").alias("hc"),
                        col("data.nox").alias("nox"),
                        col("data.pmx").alias("pmx"),
                        col("data.noise").alias("noise"),
                        to_timestamp(
                                from_unixtime(col("data.timestamp").divide(1000))
                        ).alias("event_time")
                );

        Column windowColumn = buildWindowColumn(windowConfig);

        Dataset<Row> result = emissions
                .withWatermark("event_time", windowConfig.getWatermarkDelay())
                .crossJoin(locationsDataset(sparkSession))
                .filter(
                        Analysis.haversineMeters(
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
                        avg("co2").alias("avg_co2"),
                        avg("co").alias("avg_co"),
                        avg("hc").alias("avg_hc"),
                        avg("nox").alias("avg_nox"),
                        avg("pmx").alias("avg_pmx"),
                        avg("noise").alias("avg_noise"),

                        max("co2").alias("max_co2"),
                        max("co").alias("max_co"),
                        max("hc").alias("max_hc"),
                        max("nox").alias("max_nox"),
                        max("pmx").alias("max_pmx"),
                        max("noise").alias("max_noise")
                );

        return result.select(
                concat_ws(
                        "/",
                        lit(NAME),
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
                                col("avg_co2"),
                                col("avg_co"),
                                col("avg_hc"),
                                col("avg_nox"),
                                col("avg_pmx"),
                                col("avg_noise"),
                                col("max_co2"),
                                col("max_co"),
                                col("max_hc"),
                                col("max_nox"),
                                col("max_pmx"),
                                col("max_noise"),
                                lit(NAME).alias("analysis_type")
                        )
                ).alias("value")
        );
    }
}
