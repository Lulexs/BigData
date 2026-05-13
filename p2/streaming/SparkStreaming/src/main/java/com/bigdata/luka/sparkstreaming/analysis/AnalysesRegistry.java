package com.bigdata.luka.sparkstreaming.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AnalysesRegistry {

    private final List<Analysis> analyses = new ArrayList<>();

    public AnalysesRegistry() {
        analyses.add(new TrafficAnalysis());
    }

    public List<Analysis> getActiveAnalyses(List<String> names) {
        return analyses.stream()
                .filter(x -> names.contains(x.getAnalysisName()))
                .collect(Collectors.toList());
    }

}
