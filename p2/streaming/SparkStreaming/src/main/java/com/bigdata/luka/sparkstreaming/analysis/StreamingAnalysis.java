package com.bigdata.luka.sparkstreaming.analysis;

import com.bigdata.luka.common.config.WindowConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface StreamingAnalysis {

    String name();

    Dataset<Row> analyze(AnalysisContext ctx, WindowConfig windowConfig);

}


