package com.bigdata.luka.flinkstreaming.analysis;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AnalysisRegistry {

    private final Map<String, StreamingAnalysis<?, ?>> analyses;

    public AnalysisRegistry(List<StreamingAnalysis<?, ?>> analysisList) {
        this.analyses = analysisList.stream()
                .collect(Collectors.toMap(
                        a -> a.name().toLowerCase(),
                        Function.identity()
                ));
    }

    public StreamingAnalysis<?, ?> get(String name) {
        StreamingAnalysis<?, ?> analysis = analyses.get(name.toLowerCase());
        if (analysis == null) {
            throw new IllegalArgumentException("Unknown analysis: " + name);
        }
        return analysis;
    }

    public List<StreamingAnalysis<?, ?>> getAll(List<String> names) {
        return names.stream()
                .map(this::get)
                .collect(Collectors.toList());
    }
}

