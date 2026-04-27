package com.bigdata.luka.flinkstreaming.analysis;

import com.bigdata.luka.flinkstreaming.parser.EmissionEvent;
import com.bigdata.luka.flinkstreaming.parser.FcdEvent;
import lombok.Getter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Getter
public class AnalysisContext {

    private final StreamExecutionEnvironment env;
    private final DataStream<EmissionEvent> emissions;
    private final DataStream<FcdEvent> fcd;

    public AnalysisContext(StreamExecutionEnvironment env, DataStream<EmissionEvent> emissions, DataStream<FcdEvent> fcd) {
        this.env = env;
        this.emissions = emissions;
        this.fcd = fcd;
    }

}
