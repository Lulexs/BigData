package com.bigdata.luka.sparkstreaming.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AnalysisContext {
    private final SparkSession spark;
    private final Dataset<Row> emissions;
    private final Dataset<Row> fcd;

    public AnalysisContext(SparkSession spark, Dataset<Row> emissions, Dataset<Row> fcd) {
        this.spark = spark;
        this.emissions = emissions;
        this.fcd = fcd;

    }

    public SparkSession spark() {
        return spark;
    }

    public Dataset<Row> emissions() {
        return emissions;
    }

    public Dataset<Row> fcd() {
        return fcd;
    }
}