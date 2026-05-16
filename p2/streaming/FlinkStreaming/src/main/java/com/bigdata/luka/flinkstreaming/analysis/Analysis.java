package com.bigdata.luka.flinkstreaming.analysis;

import com.bigdata.luka.common.config.WindowConfig;
import com.bigdata.luka.flinkstreaming.KafkaInputEvent;
import com.bigdata.luka.flinkstreaming.KafkaOutputEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;

import java.time.Duration;

public interface Analysis {
    double RADIUS = 200;

    String getAnalysisName();

    DataStream<KafkaOutputEvent> analyze(DataStream<KafkaInputEvent> inputStream, WindowConfig windowConfig);

    static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        final double earthRadiusMeters = 6371000.0;

        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);
        double deltaLat = Math.toRadians(lat2 - lat1);
        double deltaLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2)
                + Math.cos(lat1Rad) * Math.cos(lat2Rad)
                * Math.sin(deltaLon / 2) * Math.sin(deltaLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return earthRadiusMeters * c;
    }

    default WindowAssigner buildWindowAssigner(WindowConfig windowConfig) {
        if (windowConfig.isTumbling()) {
            return TumblingEventTimeWindows.of(Duration.ofSeconds(windowConfig.length()));
        }

        if (windowConfig.isSliding()) {
            return SlidingEventTimeWindows.of(
                    Duration.ofSeconds(windowConfig.slide()),
                    Duration.ofSeconds(windowConfig.length())
            );
        }

        throw new IllegalArgumentException(
                "Unsupported window type: " + windowConfig.getType()
        );
    }

}
