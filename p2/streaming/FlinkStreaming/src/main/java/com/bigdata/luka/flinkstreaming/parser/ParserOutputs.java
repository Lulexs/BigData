package com.bigdata.luka.flinkstreaming.parser;

import org.apache.flink.util.OutputTag;

public class ParserOutputs {

    public static final OutputTag<EmissionEvent> EMISSIONS =
            new OutputTag<>("emissions") {};

    public static final OutputTag<FcdEvent> FCD =
            new OutputTag<>("fcd") {};
}
