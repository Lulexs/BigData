package com.bigdata.luka.sparkstreaming.analysis.impl;

import com.bigdata.luka.common.config.WindowConfig;
import com.bigdata.luka.sparkstreaming.analysis.AnalysisContext;
import com.bigdata.luka.sparkstreaming.analysis.StreamingAnalysis;
import com.bigdata.luka.sparkstreaming.analysis.WindowHelper;
import com.bigdata.luka.sparkstreaming.geo.GeoFunctions;
import com.bigdata.luka.sparkstreaming.geo.SpatialEntities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.round;

public class PollutionAnalysis implements StreamingAnalysis {

    @Override
    public String name() {
        return "pollution";
    }

    @Override
    public Dataset<Row> analyze(AnalysisContext ctx, WindowConfig windowConfig) {
        Dataset<Row> points = SpatialEntities.asDataFrame(ctx.spark()).alias("p");
        Dataset<Row> events = ctx.emissions().alias("e");

        double radiusMeters = 200;
        Dataset<Row> spatialMatches = events.crossJoin(broadcast(points))
                .withColumn(
                        "distanceMeters",
                        GeoFunctions.distance(
                                col("e.y"),
                                col("e.x"),
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
                .agg(
                        avg("e.co2").alias("avgCo2"),
                        avg("e.co").alias("avgCo"),
                        avg("e.hc").alias("avgHc"),
                        avg("e.nox").alias("avgNox"),
                        avg("e.pmx").alias("avgPmx"),
                        avg("e.noise").alias("avgNoise"),
                        count("*").alias("eventCount")
                )
                .select(
                        col("window.start").alias("windowStart"),
                        col("window.end").alias("windowEnd"),
                        col("entityId"),
                        col("lat").alias("entityLat"),
                        col("lon").alias("entityLon"),
                        round(col("avgCo2"), 4).alias("avgCo2"),
                        round(col("avgCo"), 4).alias("avgCo"),
                        round(col("avgHc"), 4).alias("avgHc"),
                        round(col("avgNox"), 4).alias("avgNox"),
                        round(col("avgPmx"), 4).alias("avgPmx"),
                        round(col("avgNoise"), 4).alias("avgNoise"),
                        col("eventCount"),
                        lit(radiusMeters).alias("radiusMeters"),
                        lit(name()).alias("analysisType")
                );
    }
}