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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public class TrafficAnalysis implements Analysis {

    private static final String NAME = "traffic";

    @Override
    public String getAnalysisName() {
        return NAME;
    }

    @Override
    public DataStream<KafkaOutputEvent> analyze(DataStream<KafkaInputEvent> inputStream, WindowConfig windowConfig) {
        DataStream<FcdEvent> fcd = inputStream
                .map(new ToFcdEventMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<FcdEvent>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(windowConfig.watermarkDelay())
                                )
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<FcdEvent>) (event, previousTimestamp) ->
                                                event.timestamp
                                )
                );

        DataStream<VehicleAtLocation> vehiclesNearLocations = fcd.flatMap(new MatchVehicleToLocationsFunction());

        DataStream<TrafficWindowResult> result = vehiclesNearLocations
                .keyBy(new LocationKeySelector())
                .window(buildWindowAssigner(windowConfig))
                .aggregate(
                        new DistinctVehicleCountAggregate(),
                        new AddWindowAndLocationFunction()
                );

        return result.map(new TrafficResultToKafkaOutputMapFunction());
    }

    public static class LocationKeySelector implements KeySelector<VehicleAtLocation, Tuple2<Double, Double>> {

        @Override
        public Tuple2<Double, Double> getKey(VehicleAtLocation value) {
            return Tuple2.of(value.locationX, value.locationY);
        }
    }

    public static class ToFcdEventMapFunction implements MapFunction<KafkaInputEvent, FcdEvent> {

        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public FcdEvent map(KafkaInputEvent value) throws Exception {
            return mapper.readValue(value.value, FcdEvent.class);
        }
    }

    public static class MatchVehicleToLocationsFunction implements FlatMapFunction<FcdEvent, VehicleAtLocation> {

        @Override
        public void flatMap(FcdEvent event, Collector<VehicleAtLocation> out) {
            for (double[] location : LocationsList.POINTS) {
                double distance = Analysis.haversineMeters(
                        event.y,
                        event.x,
                        location[0],
                        location[1]
                );

                if (distance <= RADIUS) {
                    VehicleAtLocation matched = new VehicleAtLocation();
                    matched.vehicleId = event.vehicleId;
                    matched.vehicleLat = event.y;
                    matched.vehicleLon = event.x;
                    matched.locationX = location[1];
                    matched.locationY = location[0];
                    matched.eventTime = event.timestamp;

                    out.collect(matched);
                }
            }
        }
    }

    public static class DistinctVehicleCountAggregate implements AggregateFunction<VehicleAtLocation, Set<String>, Long> {

        @Override
        public Set<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<String> add(VehicleAtLocation value, Set<String> accumulator) {
            accumulator.add(value.vehicleId);
            return accumulator;
        }

        @Override
        public Long getResult(Set<String> accumulator) {
            return (long) accumulator.size();
        }

        @Override
        public Set<String> merge(Set<String> a, Set<String> b) {
            a.addAll(b);
            return a;
        }
    }

    public static class AddWindowAndLocationFunction extends ProcessWindowFunction<Long, TrafficWindowResult, Tuple2<Double, Double>, TimeWindow> {

        @Override
        public void process(
                Tuple2<Double, Double> key,
                Context context,
                Iterable<Long> elements,
                Collector<TrafficWindowResult> out
        ) {
            Long count = elements.iterator().next();

            TrafficWindowResult result = new TrafficWindowResult();
            result.windowStart = context.window().getStart();
            result.windowEnd = context.window().getEnd();
            result.locationX = key.f0;
            result.locationY = key.f1;
            result.distinctCarsCount = count;
            result.analysisType = NAME;

            out.collect(result);
        }
    }

    public static class TrafficResultToKafkaOutputMapFunction implements MapFunction<TrafficWindowResult, KafkaOutputEvent> {

        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public KafkaOutputEvent map(TrafficWindowResult value) throws Exception {
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
            payload.put("distinct_cars_count", value.distinctCarsCount);
            payload.put("analysis_type", value.analysisType);

            String kafkaValue = mapper.writeValueAsString(payload);

            return new KafkaOutputEvent(key, kafkaValue);
        }
    }

    public static class FcdEvent {
        public Long timestep;
        public String id;
        public Double x;
        public Double y;
        public Double angle;
        public String type;
        public Double speed;
        public Double pos;
        public String lane;
        public Double slope;
        public String key;
        public Long timestamp;
        public String vehicleId;
    }

    public static class VehicleAtLocation {
        public String vehicleId;
        public Double vehicleLat;
        public Double vehicleLon;
        public Double locationX;
        public Double locationY;
        public Long eventTime;
    }

    public static class TrafficWindowResult {
        public Long windowStart;
        public Long windowEnd;
        public Double locationX;
        public Double locationY;
        public Long distinctCarsCount;
        public String analysisType;
    }
}