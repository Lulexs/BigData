package com.bigdata.luka.flinkstreaming.analysis.impl;

import com.bigdata.luka.common.config.WindowConfig;
import com.bigdata.luka.flinkstreaming.analysis.AnalysisContext;
import com.bigdata.luka.flinkstreaming.analysis.StreamingAnalysis;
import com.bigdata.luka.flinkstreaming.analysis.WindowHelper;
import com.bigdata.luka.flinkstreaming.geo.GeoFunctions;
import com.bigdata.luka.flinkstreaming.geo.SpatialEntities;
import com.bigdata.luka.flinkstreaming.model.Match;
import com.bigdata.luka.flinkstreaming.model.TrafficResult;
import com.bigdata.luka.flinkstreaming.parser.FcdEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TrafficAnalysis implements StreamingAnalysis<FcdEvent, TrafficResult> {

    private static final double RADIUS_METERS = 200.0;

    @Override
    public String name() {
        return "traffic";
    }

    @Override
    public DataStream<TrafficResult> analyze(AnalysisContext ctx, WindowConfig windowConfig) {
        return ctx.getFcd()
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<FcdEvent>forBoundedOutOfOrderness(Duration.of(windowConfig.watermarkDelay(), ChronoUnit.SECONDS))
                                .withTimestampAssigner((SerializableTimestampAssigner<FcdEvent>) (event, timestamp) -> event.eventTime)
                )
                .flatMap(new SpatialMatchFunction())
                .keyBy(match -> match.entityId)
                .window(WindowHelper.<Match>create(windowConfig))
                .aggregate(new TrafficAggregateFunction(), new TrafficWindowFunction());
    }

    public static class TrafficAccumulator {
        public int entityId;
        public double entityLat;
        public double entityLon;
        public Set<String> vehicleIds = new HashSet<>();
    }

    private static class SpatialMatchFunction extends RichFlatMapFunction<FcdEvent, Match> {

        private transient List<SpatialEntities.SpatialPoint> points;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            points = SpatialEntities.getPoints();
        }

        @Override
        public void flatMap(FcdEvent event, Collector<Match> out) {
            if (event.lat == null || event.lon == null) {
                return;
            }

            for (SpatialEntities.SpatialPoint point : points) {
                double distanceMeters = GeoFunctions.distance(event.lat, event.lon, point.lat, point.lon);
                if (distanceMeters <= RADIUS_METERS) {
                    out.collect(new Match(event.vehicleId, event.eventTime, point.entityId, point.lat, point.lon));
                }
            }
        }
    }

    private static class TrafficAggregateFunction
            implements AggregateFunction<Match, TrafficAccumulator, TrafficAccumulator> {

        @Override
        public TrafficAccumulator createAccumulator() {
            return new TrafficAccumulator();
        }

        @Override
        public TrafficAccumulator add(Match value, TrafficAccumulator acc) {
            acc.entityId = value.entityId;
            acc.entityLat = value.lat;
            acc.entityLon = value.lon;
            if (value.vehicleId != null) {
                acc.vehicleIds.add(value.vehicleId);
            }
            return acc;
        }

        @Override
        public TrafficAccumulator getResult(TrafficAccumulator acc) {
            return acc;
        }

        @Override
        public TrafficAccumulator merge(TrafficAccumulator a, TrafficAccumulator b) {
            a.vehicleIds.addAll(b.vehicleIds);
            return a;
        }
    }

    private static class TrafficWindowFunction
            extends ProcessWindowFunction<TrafficAccumulator, TrafficResult, Integer, TimeWindow> {

        @Override
        public void process(
                Integer entityId,
                Context context,
                Iterable<TrafficAccumulator> elements,
                Collector<TrafficResult> out) {

            TrafficAccumulator acc = elements.iterator().next();

            TrafficResult result = new TrafficResult();
            result.windowStart = context.window().getStart();
            result.windowEnd = context.window().getEnd();
            result.entityId = entityId;
            result.entityLat = acc.entityLat;
            result.entityLon = acc.entityLon;
            result.vehicleCount = acc.vehicleIds.size();
            result.radiusMeters = RADIUS_METERS;
            result.analysisType = "traffic";

            out.collect(result);
        }
    }
}
