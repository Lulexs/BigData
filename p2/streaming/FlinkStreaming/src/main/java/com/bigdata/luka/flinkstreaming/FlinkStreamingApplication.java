package com.bigdata.luka.flinkstreaming;


import com.bigdata.luka.common.config.AppConfig;
import com.bigdata.luka.flinkstreaming.analysis.AnalysesRegistry;
import com.bigdata.luka.flinkstreaming.analysis.Analysis;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FlinkStreamingApplication {

    public static void main(String[] args) throws Exception {
        AppConfig appConfig = new AppConfig();

        Map<String, String> analysisToTopic = appConfig.analysisToInputTopic()
                .entrySet()
                .stream()
                .filter(entry -> appConfig.analysisNames().contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<KafkaInputEvent> source = KafkaSource.<KafkaInputEvent>builder()
                .setBootstrapServers(appConfig.bootstrapServers())
                .setTopics(new ArrayList<>(analysisToTopic.values()))
                .setGroupId(appConfig.appName())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new KafkaInputEventDeserializer())
                .build();

        DataStream<KafkaInputEvent> baseStream = env.fromSource(
                source,
                WatermarkStrategy.forBoundedOutOfOrderness(
                        Duration.ofSeconds(appConfig.windowConfig().watermarkDelay())
                ),
                "Kafka Source"
        );

        AnalysesRegistry analysesRegistry = new AnalysesRegistry();
        List<Analysis> activeAnalyses = analysesRegistry.getActiveAnalyses(appConfig.analysisNames());

        List<DataStream<KafkaOutputEvent>> resultStreams = new ArrayList<>();

        for (Analysis analysis : activeAnalyses) {
            String analysisName = analysis.getAnalysisName();
            String inputTopic = analysisToTopic.get(analysisName);

            DataStream<KafkaInputEvent> analysisInputStream = baseStream
                    .filter(new TopicFilter(inputTopic))
                    .name("Filter topic for " + analysisName);

            DataStream<KafkaOutputEvent> analysisStream = analysis.analyze(
                    analysisInputStream,
                    appConfig.windowConfig()
            );

            resultStreams.add(analysisStream);
        }

        KafkaSink<KafkaOutputEvent> sink = KafkaSink.<KafkaOutputEvent>builder()
                .setBootstrapServers(appConfig.bootstrapServers())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<KafkaOutputEvent>builder()
                                .setTopic(appConfig.outputTopic())
                                .setKeySerializationSchema(new KafkaOutputKeySerializationSchema())
                                .setValueSerializationSchema(new KafkaOutputValueSerializationSchema())
                                .build()
                )
                .build();
        DataStream<KafkaOutputEvent> outputStream = resultStreams.get(0);

        for (int i = 1; i < resultStreams.size(); i++) {
            outputStream = outputStream.union(resultStreams.get(i));
        }

        outputStream.sinkTo(sink).name("Kafka Analysis Results Sink");

        env.execute(appConfig.appName());
    }

    public static class KafkaOutputKeySerializationSchema implements SerializationSchema<KafkaOutputEvent> {

        @Override
        public byte[] serialize(KafkaOutputEvent event) {
            if (event == null || event.key == null) {
                return null;
            }

            return event.key.getBytes(StandardCharsets.UTF_8);
        }
    }

    public static class KafkaOutputValueSerializationSchema implements SerializationSchema<KafkaOutputEvent> {

        @Override
        public byte[] serialize(KafkaOutputEvent event) {
            if (event == null || event.value == null) {
                return null;
            }

            return event.value.getBytes(StandardCharsets.UTF_8);
        }
    }

    public static class TopicFilter implements FilterFunction<KafkaInputEvent> {

        private final String topic;

        public TopicFilter(String topic) {
            this.topic = topic;
        }

        @Override
        public boolean filter(KafkaInputEvent event) {
            return event != null
                    && event.topic != null
                    && event.topic.equals(topic);
        }
    }
}
