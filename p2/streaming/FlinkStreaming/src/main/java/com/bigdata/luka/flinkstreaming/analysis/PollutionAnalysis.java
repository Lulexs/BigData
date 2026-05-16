package com.bigdata.luka.flinkstreaming.analysis;

import com.bigdata.luka.common.config.WindowConfig;
import com.bigdata.luka.common.geo.LocationsList;
import com.bigdata.luka.flinkstreaming.KafkaInputEvent;
import com.bigdata.luka.flinkstreaming.KafkaOutputEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

public class PollutionAnalysis implements Analysis {

    private static final String NAME = "pollution";

    @Override
    public String getAnalysisName() {
        return NAME;
    }

    @Override
    public DataStream<KafkaOutputEvent> analyze(DataStream<KafkaInputEvent> inputStream, WindowConfig windowConfig) {
        DataStream<EmissionEvent> emissions = inputStream
                .map(new ToEmissionEventMapFunction())
                .filter(event ->
                        event != null
                                && event.vehicleId != null
                                && event.x != null
                                && event.y != null
                                && event.timestamp != null
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<EmissionEvent>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(windowConfig.watermarkDelay())
                                )
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<EmissionEvent>) (event, previousTimestamp) ->
                                                event.timestamp
                                )
                );

        DataStream<EmissionAtLocation> emissionsNearLocations = emissions
                .flatMap(new MatchEmissionToLocationsFunction());

        DataStream<PollutionWindowResult> result = emissionsNearLocations
                .keyBy(new LocationKeySelector())
                .window(buildWindowAssigner(windowConfig))
                .aggregate(
                        new PollutionAggregateFunction(),
                        new AddWindowAndLocationFunction()
                );

        return result.map(new PollutionResultToKafkaOutputMapFunction());
    }

    public static class ToEmissionEventMapFunction
            implements MapFunction<KafkaInputEvent, EmissionEvent> {

        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public EmissionEvent map(KafkaInputEvent value) throws Exception {
            return mapper.readValue(value.value, EmissionEvent.class);
        }
    }

    public static class MatchEmissionToLocationsFunction
            implements FlatMapFunction<EmissionEvent, EmissionAtLocation> {

        @Override
        public void flatMap(EmissionEvent event, Collector<EmissionAtLocation> out) {
            for (double[] location : LocationsList.POINTS) {
                double distance = Analysis.haversineMeters(
                        event.y,
                        event.x,
                        location[0],
                        location[1]
                );

                if (distance <= RADIUS) {
                    EmissionAtLocation matched = new EmissionAtLocation();

                    matched.vehicleId = event.vehicleId;
                    matched.vehicleLat = event.y;
                    matched.vehicleLon = event.x;

                    matched.locationX = location[1];
                    matched.locationY = location[0];

                    matched.co2 = event.co2;
                    matched.co = event.co;
                    matched.hc = event.hc;
                    matched.nox = event.nox;
                    matched.pmx = event.pmx;
                    matched.noise = event.noise;

                    matched.eventTime = event.timestamp;

                    out.collect(matched);
                }
            }
        }
    }

    public static class LocationKeySelector
            implements KeySelector<EmissionAtLocation, Tuple2<Double, Double>> {

        @Override
        public Tuple2<Double, Double> getKey(EmissionAtLocation value) {
            return Tuple2.of(value.locationX, value.locationY);
        }
    }

    public static class PollutionAggregateFunction
            implements AggregateFunction<EmissionAtLocation, PollutionAccumulator, PollutionAggregateResult> {

        @Override
        public PollutionAccumulator createAccumulator() {
            PollutionAccumulator acc = new PollutionAccumulator();

            acc.maxCo2 = Double.NEGATIVE_INFINITY;
            acc.maxCo = Double.NEGATIVE_INFINITY;
            acc.maxHc = Double.NEGATIVE_INFINITY;
            acc.maxNox = Double.NEGATIVE_INFINITY;
            acc.maxPmx = Double.NEGATIVE_INFINITY;
            acc.maxNoise = Double.NEGATIVE_INFINITY;

            return acc;
        }

        @Override
        public PollutionAccumulator add(EmissionAtLocation value, PollutionAccumulator acc) {
            acc.count++;

            if (value.co2 != null) {
                acc.sumCo2 += value.co2;
                acc.maxCo2 = Math.max(acc.maxCo2, value.co2);
            }

            if (value.co != null) {
                acc.sumCo += value.co;
                acc.maxCo = Math.max(acc.maxCo, value.co);
            }

            if (value.hc != null) {
                acc.sumHc += value.hc;
                acc.maxHc = Math.max(acc.maxHc, value.hc);
            }

            if (value.nox != null) {
                acc.sumNox += value.nox;
                acc.maxNox = Math.max(acc.maxNox, value.nox);
            }

            if (value.pmx != null) {
                acc.sumPmx += value.pmx;
                acc.maxPmx = Math.max(acc.maxPmx, value.pmx);
            }

            if (value.noise != null) {
                acc.sumNoise += value.noise;
                acc.maxNoise = Math.max(acc.maxNoise, value.noise);
            }

            return acc;
        }

        @Override
        public PollutionAggregateResult getResult(PollutionAccumulator acc) {
            PollutionAggregateResult result = new PollutionAggregateResult();

            if (acc.count == 0) {
                return result;
            }

            result.avgCo2 = acc.sumCo2 / acc.count;
            result.avgCo = acc.sumCo / acc.count;
            result.avgHc = acc.sumHc / acc.count;
            result.avgNox = acc.sumNox / acc.count;
            result.avgPmx = acc.sumPmx / acc.count;
            result.avgNoise = acc.sumNoise / acc.count;

            result.maxCo2 = normalizeMax(acc.maxCo2);
            result.maxCo = normalizeMax(acc.maxCo);
            result.maxHc = normalizeMax(acc.maxHc);
            result.maxNox = normalizeMax(acc.maxNox);
            result.maxPmx = normalizeMax(acc.maxPmx);
            result.maxNoise = normalizeMax(acc.maxNoise);

            return result;
        }

        @Override
        public PollutionAccumulator merge(PollutionAccumulator a, PollutionAccumulator b) {
            a.count += b.count;

            a.sumCo2 += b.sumCo2;
            a.sumCo += b.sumCo;
            a.sumHc += b.sumHc;
            a.sumNox += b.sumNox;
            a.sumPmx += b.sumPmx;
            a.sumNoise += b.sumNoise;

            a.maxCo2 = Math.max(a.maxCo2, b.maxCo2);
            a.maxCo = Math.max(a.maxCo, b.maxCo);
            a.maxHc = Math.max(a.maxHc, b.maxHc);
            a.maxNox = Math.max(a.maxNox, b.maxNox);
            a.maxPmx = Math.max(a.maxPmx, b.maxPmx);
            a.maxNoise = Math.max(a.maxNoise, b.maxNoise);

            return a;
        }

        private static Double normalizeMax(double value) {
            if (value == Double.NEGATIVE_INFINITY) {
                return null;
            }

            return value;
        }
    }

    public static class AddWindowAndLocationFunction
            extends ProcessWindowFunction<
            PollutionAggregateResult,
            PollutionWindowResult,
            Tuple2<Double, Double>,
            TimeWindow> {

        @Override
        public void process(
                Tuple2<Double, Double> key,
                Context context,
                Iterable<PollutionAggregateResult> elements,
                Collector<PollutionWindowResult> out
        ) {
            PollutionAggregateResult aggregate = elements.iterator().next();

            PollutionWindowResult result = new PollutionWindowResult();

            result.windowStart = context.window().getStart();
            result.windowEnd = context.window().getEnd();

            result.locationX = key.f0;
            result.locationY = key.f1;

            result.avgCo2 = aggregate.avgCo2;
            result.avgCo = aggregate.avgCo;
            result.avgHc = aggregate.avgHc;
            result.avgNox = aggregate.avgNox;
            result.avgPmx = aggregate.avgPmx;
            result.avgNoise = aggregate.avgNoise;

            result.maxCo2 = aggregate.maxCo2;
            result.maxCo = aggregate.maxCo;
            result.maxHc = aggregate.maxHc;
            result.maxNox = aggregate.maxNox;
            result.maxPmx = aggregate.maxPmx;
            result.maxNoise = aggregate.maxNoise;

            result.analysisType = NAME;

            out.collect(result);
        }
    }

    public static class PollutionResultToKafkaOutputMapFunction
            implements MapFunction<PollutionWindowResult, KafkaOutputEvent> {

        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public KafkaOutputEvent map(PollutionWindowResult value) throws Exception {
            String windowStart = Instant.ofEpochMilli(value.windowStart).toString();
            String windowEnd = Instant.ofEpochMilli(value.windowEnd).toString();

            String key = String.join(
                    "/",
                    NAME,
                    windowStart,
                    windowEnd,
                    String.valueOf(value.locationX),
                    String.valueOf(value.locationY)
            );

            ObjectNode payload = mapper.createObjectNode();

            payload.put("window_start", windowStart);
            payload.put("window_end", windowEnd);
            payload.put("location_x", value.locationX);
            payload.put("location_y", value.locationY);

            putNullable(payload, "avg_co2", value.avgCo2);
            putNullable(payload, "avg_co", value.avgCo);
            putNullable(payload, "avg_hc", value.avgHc);
            putNullable(payload, "avg_nox", value.avgNox);
            putNullable(payload, "avg_pmx", value.avgPmx);
            putNullable(payload, "avg_noise", value.avgNoise);

            putNullable(payload, "max_co2", value.maxCo2);
            putNullable(payload, "max_co", value.maxCo);
            putNullable(payload, "max_hc", value.maxHc);
            putNullable(payload, "max_nox", value.maxNox);
            putNullable(payload, "max_pmx", value.maxPmx);
            putNullable(payload, "max_noise", value.maxNoise);

            payload.put("analysis_type", value.analysisType);

            String kafkaValue = mapper.writeValueAsString(payload);

            return new KafkaOutputEvent(key, kafkaValue);
        }

        private static void putNullable(ObjectNode node, String fieldName, Double value) {
            if (value == null) {
                node.putNull(fieldName);
            } else {
                node.put(fieldName, value);
            }
        }
    }

    public static class EmissionEvent {
        public Long timestep;
        public String id;
        public String eclass;
        public Double co2;
        public Double co;
        public Double hc;
        public Double nox;
        public Double pmx;
        public Double fuel;
        public Double electricity;
        public Double noise;
        public String route;
        public String type;
        public Double waiting;
        public String lane;
        public Double pos;
        public Double speed;
        public Double angle;
        public Double x;
        public Double y;
        public String key;
        public Long timestamp;
        public String vehicleId;
    }

    public static class EmissionAtLocation {
        public String vehicleId;
        public Double vehicleLat;
        public Double vehicleLon;

        public Double locationX;
        public Double locationY;

        public Double co2;
        public Double co;
        public Double hc;
        public Double nox;
        public Double pmx;
        public Double noise;

        public Long eventTime;
    }

    public static class PollutionAccumulator {
        public long count;

        public double sumCo2;
        public double sumCo;
        public double sumHc;
        public double sumNox;
        public double sumPmx;
        public double sumNoise;

        public double maxCo2;
        public double maxCo;
        public double maxHc;
        public double maxNox;
        public double maxPmx;
        public double maxNoise;
    }

    public static class PollutionAggregateResult {
        public Double avgCo2;
        public Double avgCo;
        public Double avgHc;
        public Double avgNox;
        public Double avgPmx;
        public Double avgNoise;

        public Double maxCo2;
        public Double maxCo;
        public Double maxHc;
        public Double maxNox;
        public Double maxPmx;
        public Double maxNoise;
    }

    public static class PollutionWindowResult {
        public Long windowStart;
        public Long windowEnd;

        public Double locationX;
        public Double locationY;

        public Double avgCo2;
        public Double avgCo;
        public Double avgHc;
        public Double avgNox;
        public Double avgPmx;
        public Double avgNoise;

        public Double maxCo2;
        public Double maxCo;
        public Double maxHc;
        public Double maxNox;
        public Double maxPmx;
        public Double maxNoise;

        public String analysisType;
    }
}