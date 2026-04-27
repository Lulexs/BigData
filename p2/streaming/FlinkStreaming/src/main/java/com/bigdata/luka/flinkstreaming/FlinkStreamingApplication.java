package com.bigdata.luka.flinkstreaming;

import com.bigdata.luka.common.config.AppConfig;
import com.bigdata.luka.flinkstreaming.analysis.AnalysisContext;
import com.bigdata.luka.flinkstreaming.analysis.AnalysisRegistry;
import com.bigdata.luka.flinkstreaming.analysis.StreamingAnalysis;
import com.bigdata.luka.flinkstreaming.parser.EmissionEvent;
import com.bigdata.luka.flinkstreaming.parser.FcdEvent;
import com.bigdata.luka.flinkstreaming.parser.ParserOutputs;
import com.bigdata.luka.flinkstreaming.parser.UnifiedEventParser;
import com.bigdata.luka.flinkstreaming.sink.KafkaResultSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class FlinkStreamingApplication {

	public static void main(String[] args) throws Exception {
		AppConfig cfg = new AppConfig();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);

		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers(cfg.bootstrapServers())
				.setTopics(cfg.inputTopics())
				.setGroupId("flink-consumer")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> kafkaStream  = env.fromSource(source,
				WatermarkStrategy.forBoundedOutOfOrderness(Duration.of(cfg.windowConfig().watermarkDelay(), ChronoUnit.SECONDS)),
				"Kafka Source");
		SingleOutputStreamOperator<Void> parsed = kafkaStream.process(new UnifiedEventParser());

		DataStream<EmissionEvent> emissions = parsed.getSideOutput(ParserOutputs.EMISSIONS);
		DataStream<FcdEvent> fcd = parsed.getSideOutput(ParserOutputs.FCD);

		AnalysisContext ctx = new AnalysisContext(env, emissions, fcd);

		List<StreamingAnalysis<?, ?>> analyses = List.of();
		AnalysisRegistry registry = new AnalysisRegistry(analyses);
		List<StreamingAnalysis<?, ?>> enabledAnalyses = registry.getAll(cfg.analysisNames());

		for (StreamingAnalysis<?, ?> analysis : enabledAnalyses) {

			DataStream<?> result = analysis.analyze(ctx, cfg.windowConfig());

			result.sinkTo(KafkaResultSink.create(cfg.bootstrapServers(), cfg.outputTopic(), analysis.name()));

			System.out.println("Started analysis: " + analysis.name());
		}

		env.execute(cfg.appName());

	}

}
