package com.bigdata.luka.flinkstreaming.analysis;

import com.bigdata.luka.common.config.WindowConfig;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public final class WindowHelper {

    private WindowHelper() {
        /* This utility class should not be instantiated */
    }

    public static <T> WindowAssigner<T, TimeWindow> create(WindowConfig cfg) {
        if (cfg.isSliding()) {
            return (WindowAssigner<T, TimeWindow>) SlidingEventTimeWindows.of(Duration.of(cfg.length(), ChronoUnit.SECONDS), Duration.of(cfg.slide(), ChronoUnit.SECONDS));
        }
        return (WindowAssigner<T, TimeWindow>) TumblingEventTimeWindows.of(Duration.of(cfg.length(), ChronoUnit.SECONDS));
    }
}
