package com.bigdata.luka.flinkstreaming;

import com.bigdata.luka.common.config.AppConfig;
import com.bigdata.luka.flinkstreaming.analysis.AnalysisContext;
import com.bigdata.luka.flinkstreaming.analysis.AnalysisRegistry;
import com.bigdata.luka.flinkstreaming.analysis.StreamingAnalysis;
import com.bigdata.luka.flinkstreaming.analysis.impl.PollutionAnalysis;
import com.bigdata.luka.flinkstreaming.analysis.impl.TrafficAnalysis;
import com.bigdata.luka.flinkstreaming.parser.EmissionEvent;
import com.bigdata.luka.flinkstreaming.parser.FcdEvent;
import com.bigdata.luka.flinkstreaming.parser.ParserOutputs;
import com.bigdata.luka.flinkstreaming.parser.UnifiedEventParser;
import com.bigdata.luka.flinkstreaming.sink.KafkaResultSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class FlinkStreamingApplication {

	public static void main(String[] args) throws Exception {
		AppConfig cfg = new AppConfig();

		Configuration flinkConfig = new Configuration();
		flinkConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
		flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, cfg.checkpointLocation());

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
		env.enableCheckpointing(5000);

		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers(cfg.bootstrapServers())
				.setTopics(cfg.inputTopics())
				.setGroupId("flink-consumer")
				.setStartingOffsets(startingOffsets(cfg.startingOffsets()))
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> kafkaStream  = env.fromSource(source,
				WatermarkStrategy.noWatermarks(),
				"Kafka Source");
		SingleOutputStreamOperator<Void> parsed = kafkaStream.process(new UnifiedEventParser());

		DataStream<EmissionEvent> emissions = parsed.getSideOutput(ParserOutputs.EMISSIONS);
		DataStream<FcdEvent> fcd = parsed.getSideOutput(ParserOutputs.FCD);

		AnalysisContext ctx = new AnalysisContext(env, emissions, fcd);

		List<StreamingAnalysis<?, ?>> analyses = List.of(
				new PollutionAnalysis(),
				new TrafficAnalysis()
		);
		AnalysisRegistry registry = new AnalysisRegistry(analyses);
		List<StreamingAnalysis<?, ?>> enabledAnalyses = registry.getAll(cfg.analysisNames());

		for (StreamingAnalysis<?, ?> analysis : enabledAnalyses) {

			DataStream<?> result = analysis.analyze(ctx, cfg.windowConfig());

			result.sinkTo(KafkaResultSink.create(cfg.bootstrapServers(), cfg.outputTopic(), analysis.name()));

			System.out.println("Started analysis: " + analysis.name());
		}

		env.execute(cfg.appName());

	}

	private static OffsetsInitializer startingOffsets(String value) {
		if ("latest".equalsIgnoreCase(value)) {
			return OffsetsInitializer.latest();
		}
		if ("earliest".equalsIgnoreCase(value)) {
			return OffsetsInitializer.earliest();
		}
		throw new IllegalArgumentException("Unsupported kafka.startingOffsets: " + value);
	}

}
