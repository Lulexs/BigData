package com.bigdata.luka.flinkstreaming.analysis.impl;

import com.bigdata.luka.common.config.WindowConfig;
import com.bigdata.luka.flinkstreaming.analysis.AnalysisContext;
import com.bigdata.luka.flinkstreaming.analysis.StreamingAnalysis;
import com.bigdata.luka.flinkstreaming.analysis.WindowHelper;
import com.bigdata.luka.flinkstreaming.geo.GeoFunctions;
import com.bigdata.luka.flinkstreaming.geo.SpatialEntities;
import com.bigdata.luka.flinkstreaming.model.PollutionResult;
import com.bigdata.luka.flinkstreaming.parser.EmissionEvent;
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
import java.util.List;

public class PollutionAnalysis implements StreamingAnalysis<EmissionEvent, PollutionResult> {

    private static final double RADIUS_METERS = 200.0;

    @Override
    public String name() {
        return "pollution";
    }

    @Override
    public DataStream<PollutionResult> analyze(AnalysisContext ctx, WindowConfig windowConfig) {
        return ctx.getEmissions()
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<EmissionEvent>forBoundedOutOfOrderness(Duration.of(windowConfig.watermarkDelay(), ChronoUnit.SECONDS))
                                .withTimestampAssigner((SerializableTimestampAssigner<EmissionEvent>) (event, timestamp) -> event.eventTime)
                )
                .flatMap(new SpatialMatchFunction())
                .keyBy(match -> match.entityId)
                .window(WindowHelper.<SpatialMatch>create(windowConfig))
                .aggregate(new PollutionAggregateFunction(), new PollutionWindowFunction());
    }

    private static double average(double sum, long count) {
        return count == 0 ? 0.0 : round4(sum / count);
    }

    private static double round4(double value) {
        return Math.round(value * 10_000.0) / 10_000.0;
    }

    public static class SpatialMatch {
        public int entityId;
        public double entityLat;
        public double entityLon;
        public Double co2;
        public Double co;
        public Double hc;
        public Double nox;
        public Double pmx;
        public Double noise;

        public SpatialMatch() {
        }

        public SpatialMatch(SpatialEntities.SpatialPoint point, EmissionEvent event) {
            this.entityId = point.entityId;
            this.entityLat = point.lat;
            this.entityLon = point.lon;
            this.co2 = event.co2;
            this.co = event.co;
            this.hc = event.hc;
            this.nox = event.nox;
            this.pmx = event.pmx;
            this.noise = event.noise;
        }
    }

    public static class PollutionAccumulator {
        public int entityId;
        public double entityLat;
        public double entityLon;
        public double co2Sum;
        public double coSum;
        public double hcSum;
        public double noxSum;
        public double pmxSum;
        public double noiseSum;
        public long co2Count;
        public long coCount;
        public long hcCount;
        public long noxCount;
        public long pmxCount;
        public long noiseCount;
        public long eventCount;
    }

    private static class SpatialMatchFunction extends RichFlatMapFunction<EmissionEvent, SpatialMatch> {

        private transient List<SpatialEntities.SpatialPoint> points;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            points = SpatialEntities.getPoints();
        }

        @Override
        public void flatMap(EmissionEvent event, Collector<SpatialMatch> out) {
            if (event.x == null || event.y == null) {
                return;
            }

            for (SpatialEntities.SpatialPoint point : points) {
                double distanceMeters = GeoFunctions.distance(event.y, event.x, point.lat, point.lon);
                if (distanceMeters <= RADIUS_METERS) {
                    out.collect(new SpatialMatch(point, event));
                }
            }
        }
    }

    private static class PollutionAggregateFunction
            implements AggregateFunction<SpatialMatch, PollutionAccumulator, PollutionAccumulator> {

        @Override
        public PollutionAccumulator createAccumulator() {
            return new PollutionAccumulator();
        }

        @Override
        public PollutionAccumulator add(SpatialMatch value, PollutionAccumulator acc) {
            acc.entityId = value.entityId;
            acc.entityLat = value.entityLat;
            acc.entityLon = value.entityLon;
            if (value.co2 != null) {
                acc.co2Sum += value.co2;
                acc.co2Count++;
            }
            if (value.co != null) {
                acc.coSum += value.co;
                acc.coCount++;
            }
            if (value.hc != null) {
                acc.hcSum += value.hc;
                acc.hcCount++;
            }
            if (value.nox != null) {
                acc.noxSum += value.nox;
                acc.noxCount++;
            }
            if (value.pmx != null) {
                acc.pmxSum += value.pmx;
                acc.pmxCount++;
            }
            if (value.noise != null) {
                acc.noiseSum += value.noise;
                acc.noiseCount++;
            }
            acc.eventCount++;
            return acc;
        }

        @Override
        public PollutionAccumulator getResult(PollutionAccumulator acc) {
            return acc;
        }

        @Override
        public PollutionAccumulator merge(PollutionAccumulator a, PollutionAccumulator b) {
            a.co2Sum += b.co2Sum;
            a.coSum += b.coSum;
            a.hcSum += b.hcSum;
            a.noxSum += b.noxSum;
            a.pmxSum += b.pmxSum;
            a.noiseSum += b.noiseSum;
            a.co2Count += b.co2Count;
            a.coCount += b.coCount;
            a.hcCount += b.hcCount;
            a.noxCount += b.noxCount;
            a.pmxCount += b.pmxCount;
            a.noiseCount += b.noiseCount;
            a.eventCount += b.eventCount;
            return a;
        }
    }

    private static class PollutionWindowFunction
            extends ProcessWindowFunction<PollutionAccumulator, PollutionResult, Integer, TimeWindow> {

        @Override
        public void process(
                Integer entityId,
                Context context,
                Iterable<PollutionAccumulator> elements,
                Collector<PollutionResult> out) {

            PollutionAccumulator acc = elements.iterator().next();
            if (acc.eventCount == 0) {
                return;
            }

            PollutionResult result = new PollutionResult();
            result.windowStart = context.window().getStart();
            result.windowEnd = context.window().getEnd();
            result.entityId = entityId;
            result.entityLat = acc.entityLat;
            result.entityLon = acc.entityLon;
            result.avgCo2 = average(acc.co2Sum, acc.co2Count);
            result.avgCo = average(acc.coSum, acc.coCount);
            result.avgHc = average(acc.hcSum, acc.hcCount);
            result.avgNox = average(acc.noxSum, acc.noxCount);
            result.avgPmx = average(acc.pmxSum, acc.pmxCount);
            result.avgNoise = average(acc.noiseSum, acc.noiseCount);
            result.eventCount = acc.eventCount;
            result.radiusMeters = RADIUS_METERS;
            result.analysisType = "pollution";

            out.collect(result);
        }
    }
}
