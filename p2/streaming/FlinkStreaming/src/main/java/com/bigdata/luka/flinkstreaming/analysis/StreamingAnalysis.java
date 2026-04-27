package com.bigdata.luka.flinkstreaming.analysis;

import com.bigdata.luka.common.config.WindowConfig;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface StreamingAnalysis<T, R> {

    String name();

    DataStream<R> analyze(AnalysisContext ctx, WindowConfig windowConfig);

}
