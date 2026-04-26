package com.bigdata.luka.common.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WindowConfig {
    private String type;
    private String length;
    private String slide;
    private String watermarkDelay;

    public boolean isSliding() {
        return "sliding".equalsIgnoreCase(type);
    }

    public boolean isTumbling() {
        return "tumbling".equalsIgnoreCase(type);
    }
}
