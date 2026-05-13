package com.bigdata.luka.sparkstreaming.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TrafficAnalysis implements Analysis {

    private static final String NAME = "traffic";

    @Override
    public String getAnalysisName() {
        return NAME;
    }

    @Override
    public Dataset<Row> analyze(Dataset<Row> inputStream) {
        return inputStream
                .filter("topic = 'FCD'")
                .selectExpr(
                        "CAST(key AS STRING) AS key",
                        "CAST(value AS STRING) AS value"
                );
    }
}
