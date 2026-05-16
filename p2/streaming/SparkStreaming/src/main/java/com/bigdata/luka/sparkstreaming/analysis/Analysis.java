package com.bigdata.luka.sparkstreaming.analysis;

import com.bigdata.luka.common.config.AppConfig;
import com.bigdata.luka.common.config.WindowConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Analysis {

    String getAnalysisName();

    Dataset<Row> analyze(SparkSession session, Dataset<Row> inputStream, WindowConfig windowConfig);

}
