package com.bigdata.luka.sparkstreaming;

import com.bigdata.luka.common.config.AppConfig;
import com.bigdata.luka.sparkstreaming.analysis.AnalysesRegistry;
import com.bigdata.luka.sparkstreaming.analysis.Analysis;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkStreamingApplication {

    public static void main(String[] args) throws Exception {
        AppConfig appConfig = new AppConfig();

        SparkSession sparkSession = SparkSession.builder()
                .appName(appConfig.appName())
                .getOrCreate();

        Map<String, String> analysisToTopic = appConfig.analysisToInputTopic()
                .entrySet().stream()
                .filter(entry -> appConfig.analysisNames().contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Dataset<Row> baseStream = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", appConfig.bootstrapServers())
                .option("subscribe", String.join(",", analysisToTopic.values()))
                .option("startingOffsets", appConfig.startingOffsets())
                .load();

        AnalysesRegistry analysesRegistry = new AnalysesRegistry();
        List<Analysis> activeAnalyses = analysesRegistry.getActiveAnalyses(appConfig.analysisNames());

        for (Analysis analysis : activeAnalyses) {
            Dataset<Row> analysisResults = analysis.analyze(baseStream);

            analysisResults.writeStream()
                    .format("kafka")
                    .option("topic", appConfig.outputTopic())
                    .option("kafka.bootstrap.servers", appConfig.bootstrapServers())
                    .option("checkpointLocation", appConfig.checkpointLocation() + "/" + analysis.getAnalysisName())
                    .outputMode("append")
                    .start();
        }

        sparkSession.streams().awaitAnyTermination();
    }
}
