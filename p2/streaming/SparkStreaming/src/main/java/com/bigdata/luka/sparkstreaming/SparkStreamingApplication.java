package com.bigdata.luka.sparkstreaming;

import com.bigdata.luka.common.config.AppConfig;
import com.bigdata.luka.sparkstreaming.analysis.AnalysisContext;
import com.bigdata.luka.sparkstreaming.analysis.AnalysisRegistry;
import com.bigdata.luka.sparkstreaming.analysis.StreamingAnalysis;
import com.bigdata.luka.sparkstreaming.analysis.impl.PollutionAnalysis;
import com.bigdata.luka.sparkstreaming.analysis.impl.TrafficAnalysis;
import com.bigdata.luka.sparkstreaming.parser.EventParser;
import com.bigdata.luka.sparkstreaming.sink.KafkaResultSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.ArrayList;
import java.util.List;

public class SparkStreamingApplication {

	public static void main(String[] args) throws Exception {
		AppConfig cfg = new AppConfig();

		SparkSession spark = SparkSession.builder()
				.appName(cfg.appName())
				.master("local[*]")
				.getOrCreate();

		Dataset<Row> kafkaSource = spark.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", cfg.bootstrapServers())
				.option("subscribe", String.join(",", cfg.inputTopics()))
				.option("startingOffsets", cfg.startingOffsets())
				.load();

		Dataset<Row> emissions = EventParser.parseEmissions(kafkaSource);
		Dataset<Row> fcd = EventParser.parseFcd(kafkaSource);

		AnalysisContext ctx = new AnalysisContext(spark, emissions, fcd);

		List<StreamingAnalysis> analyses = List.of(
				new PollutionAnalysis(),
				new TrafficAnalysis()
		);

		AnalysisRegistry registry = new AnalysisRegistry(analyses);

		List<StreamingAnalysis> enabledAnalyses = registry.getAll(cfg.analysisNames());
		List<StreamingQuery> queries = new ArrayList<>();

		for (StreamingAnalysis analysis : enabledAnalyses) {
			Dataset<Row> analyzed = analysis.analyze(ctx, cfg.windowConfig());

			StreamingQuery query = KafkaResultSink.write(
					analyzed,
					cfg.bootstrapServers(),
					cfg.outputTopic(),
					cfg.checkpointLocation(),
					cfg.triggerInterval(),
					analysis.name()
			);

			queries.add(query);
			System.out.println("Started analysis: " + analysis.name());
		}

		for (StreamingQuery query : queries) {
			query.awaitTermination();
		}
	}
}
