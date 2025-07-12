package com.exness.sdp.flink.common.serde;

import org.junit.jupiter.api.Test;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static com.exness.sdp.flink.common.serde.SerdeUtils.toUpperCamelCase;
import static org.junit.jupiter.api.Assertions.assertEquals;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
class SerdeUtilsTest {

    @Test
    void testToUpperCamelCase() {
        String actual = "some random-test.string_with-mixed_case";
        String expected = "SomeRandomTestStringWithMixedCase";
        assertEquals(expected, toUpperCamelCase(actual));
    }
}
