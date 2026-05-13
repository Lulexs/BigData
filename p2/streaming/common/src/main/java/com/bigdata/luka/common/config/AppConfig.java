package com.bigdata.luka.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AppConfig {

    private final Config config = ConfigFactory.load();

    public String appName() {
        return config.getString("app.name");
    }

    public String bootstrapServers() {
        return config.getString("kafka.bootstrap.servers");
    }

    public Map<String, String> analysisToInputTopic() {
        return Arrays.stream(config.getString("analysis.topic.mappings").split(";"))
                .collect(Collectors.toMap(s -> s.split(":")[0], s -> s.split(":")[1]));
    }

    public String outputTopic() {
        return config.getString("kafka.output.topic");
    }

    public String startingOffsets() {
        return config.getString("kafka.startingOffsets");
    }

    public String checkpointLocation() {
        return config.getString("kafka.checkpointLocation");
    }

    public List<String> analysisNames() {
        return Arrays.stream(config.getString("analysis.names").split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    public String triggerInterval() {
        return config.getString("trigger.interval");
    }

    public WindowConfig windowConfig() {
        return new WindowConfig(
                config.getString("window.type"),
                config.getString("window.length"),
                config.hasPath("window.slide") ? config.getString("window.slide") : null,
                config.getString("watermark.delay")
        );
    }
}