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

    public Long length() {
        return parseSeconds(length);
    }

    public Long slide() {
        return parseSeconds(slide);
    }

    public Long watermarkDelay() { return parseSeconds(watermarkDelay); }

    private Long parseSeconds(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        int firstSpace = trimmed.indexOf(' ');
        String number = firstSpace >= 0 ? trimmed.substring(0, firstSpace) : trimmed;
        return Long.parseLong(number);
    }
}
