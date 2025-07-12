package com.exness.sdp.flink.common.serde;

import org.apache.commons.text.CaseUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SerdeUtils {
    public static String toUpperCamelCase(String value) {
        return CaseUtils.toCamelCase(value.replaceAll("[^a-zA-Z0-9]", " "), true, ' ');
    }
}
