package com.bigdata.luka.sparkstreaming.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Analysis {

    String getAnalysisName();

    Dataset<Row> analyze(Dataset<Row> inputStream);

}
