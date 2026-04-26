package com.bigdata.luka.sparkstreaming.analysis;

import com.bigdata.luka.common.config.WindowConfig;
import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.window;

public final class WindowHelper {

    private WindowHelper() {
        /* This utility class should not be instantiated */
    }

    public static Column windowColumn(WindowConfig cfg) {
        if (cfg.isSliding()) {
            return window(org.apache.spark.sql.functions.col("eventTime"), cfg.getLength(), cfg.getSlide());
        }
        if (cfg.isTumbling()) {
            return window(org.apache.spark.sql.functions.col("eventTime"), cfg.getLength());
        }
        throw new IllegalArgumentException("Unsupported window.type: " + cfg.getType());
    }
}
