package com.exness.sdp.flink.format.json.util;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonToRowDataConvertersTest implements Serializable {

    @Test
    void createConverterTest() {
        JsonToRowDataConverters converters = new JsonToRowDataConverters(true,
                true,
                TimestampFormat.ISO_8601);
        var strConverter = converters.createConverter(DataTypes.STRING().getLogicalType());
        assertEquals(StringData.fromString("test"), strConverter.convert(new TextNode("test")));
        var intConverter = converters.createConverter(DataTypes.INT().getLogicalType());
        assertEquals(1, intConverter.convert(new IntNode(1)));
        var longConverter = converters.createConverter(DataTypes.BIGINT().getLogicalType());
        assertEquals(1L, longConverter.convert(new LongNode(1L)));
        var doubleConverter = converters.createConverter(DataTypes.DOUBLE().getLogicalType());
        assertEquals(1.0, doubleConverter.convert(new DoubleNode(1.0)));
        var floatConverter = converters.createConverter(DataTypes.FLOAT().getLogicalType());
        assertEquals(1.0f, floatConverter.convert(new DoubleNode(1.0)));
        var booleanConverter = converters.createConverter(DataTypes.BOOLEAN().getLogicalType());
        assertEquals(Boolean.TRUE, booleanConverter.convert(BooleanNode.TRUE));
        var byteConverter = converters.createConverter(DataTypes.TINYINT().getLogicalType());
        assertEquals((byte) 1, byteConverter.convert(new IntNode(1)));
        var shortConverter = converters.createConverter(DataTypes.SMALLINT().getLogicalType());
        assertEquals((short) 1, shortConverter.convert(new IntNode(1)));
        var dateConverter = converters.createConverter(DataTypes.DATE().getLogicalType());
        assertEquals(1, dateConverter.convert(new TextNode("1970-01-02")));
        var timeConverter = converters.createConverter(DataTypes.TIME().getLogicalType());
        assertEquals(3600000, timeConverter.convert(new TextNode("01:00:00")));
        var timestampConverter = converters.createConverter(DataTypes.TIMESTAMP().getLogicalType());
        assertEquals(TimestampData.fromEpochMillis(0), timestampConverter.convert(new TextNode("1970-01-01T00:00:00")));
        var timestampWithLocalZoneConverter = converters.createConverter(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().getLogicalType());
        assertEquals(TimestampData.fromEpochMillis(0), timestampWithLocalZoneConverter.convert(new TextNode("1970-01-01T00:00:00Z")));
        var decimalConverter = converters.createConverter(DataTypes.DECIMAL(10, 2).getLogicalType());
        assertEquals(DecimalData.fromBigDecimal(new BigDecimal("1.00"), 10, 2), decimalConverter.convert(new DecimalNode(new BigDecimal("1.00"))));
        var arrayConverter = converters.createConverter(DataTypes.ARRAY(DataTypes.INT()).getLogicalType());
        assertEquals(new GenericArrayData(new Integer[]{1, 2, 3}), arrayConverter.convert(new ArrayNode(JsonNodeFactory.instance).add(1).add(2).add(3)));
        var mapConverter = converters.createConverter(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()).getLogicalType());
        assertEquals(new GenericMapData(Map.of(StringData.fromString("a"), 1, StringData.fromString("b"), 2)),
                mapConverter.convert(new ObjectNode(JsonNodeFactory.instance).put("a", 1).put("b", 2)));
        var rowConverter = converters.createConverter(DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT())).getLogicalType());
        GenericRowData row = new GenericRowData(1);
        row.setField(0, 1);
        assertEquals(row, rowConverter.convert(new ObjectNode(JsonNodeFactory.instance).put("a", 1)));
    }
}
