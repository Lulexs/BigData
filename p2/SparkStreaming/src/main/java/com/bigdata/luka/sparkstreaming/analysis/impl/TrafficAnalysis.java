package com.bigdata.luka.sparkstreaming.analysis.impl;

import com.bigdata.luka.sparkstreaming.analysis.AnalysisContext;
import com.bigdata.luka.sparkstreaming.analysis.StreamingAnalysis;
import com.bigdata.luka.sparkstreaming.analysis.WindowHelper;
import com.bigdata.luka.sparkstreaming.config.WindowConfig;
import com.bigdata.luka.sparkstreaming.geo.GeoFunctions;
import com.bigdata.luka.sparkstreaming.geo.SpatialEntities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.lit;

public class TrafficAnalysis implements StreamingAnalysis {

    @Override
    public String name() {
        return "traffic";
    }

    @Override
    public Dataset<Row> analyze(AnalysisContext ctx, WindowConfig windowConfig) {
        Dataset<Row> points = SpatialEntities.asDataFrame(ctx.spark()).alias("p");
        Dataset<Row> fcd = ctx.fcd().alias("f");

        double radiusMeters = 200;
        Dataset<Row> spatialMatches = fcd
                .crossJoin(broadcast(points))
                .withColumn(
                        "distanceMeters",
                        GeoFunctions.distance(
                                col("f.lat"),
                                col("f.lon"),
                                col("p.lat"),
                                col("p.lon")
                        )
                )
                .filter(col("distanceMeters").leq(radiusMeters));

        return spatialMatches
                .withWatermark("eventTime", windowConfig.getWatermarkDelay())
                .groupBy(
                        WindowHelper.windowColumn(windowConfig),
                        col("p.entityId"),
                        col("p.lat"),
                        col("p.lon")
                )
                .agg(countDistinct("f.vehicleId").alias("vehicleCount"))
                .select(
                        col("window.start").alias("windowStart"),
                        col("window.end").alias("windowEnd"),
                        col("entityId"),
                        col("lat").alias("entityLat"),
                        col("lon").alias("entityLon"),
                        col("vehicleCount"),
                        lit(radiusMeters).alias("radiusMeters"),
                        lit(name()).alias("analysisType")
                );
    }
}