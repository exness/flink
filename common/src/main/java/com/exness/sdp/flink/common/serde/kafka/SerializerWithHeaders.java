package com.exness.sdp.flink.common.serde.kafka;

import org.apache.flink.table.data.RowData;
import org.apache.kafka.common.header.Headers;

public interface SerializerWithHeaders {
    byte[] serialize(RowData rowData, Headers headers);
}
