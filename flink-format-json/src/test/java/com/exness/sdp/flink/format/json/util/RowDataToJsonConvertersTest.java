package com.exness.sdp.flink.format.json.util;

import java.math.BigDecimal;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RowDataToJsonConvertersTest {
    @Test
    void createConverterTest() {
        RowDataToJsonConverters converters = new RowDataToJsonConverters(TimestampFormat.ISO_8601, JsonFormatOptions.MapNullKeyMode.DROP,  null, true);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.createObjectNode();
        var strConverter = converters.createConverter(DataTypes.STRING().getLogicalType());
        assertEquals(new TextNode("test"), strConverter.convert(objectMapper, node, StringData.fromString("test")));
        var intConverter = converters.createConverter(DataTypes.INT().getLogicalType());
        assertEquals(new IntNode(1), intConverter.convert(objectMapper, node, 1));
        var longConverter = converters.createConverter(DataTypes.BIGINT().getLogicalType());
        assertEquals(new LongNode(1L), longConverter.convert(objectMapper, node, 1L));
        var doubleConverter = converters.createConverter(DataTypes.DOUBLE().getLogicalType());
        assertEquals(new DoubleNode(1.0), doubleConverter.convert(objectMapper, node, 1.0));
        var floatConverter = converters.createConverter(DataTypes.FLOAT().getLogicalType());
        assertEquals(new FloatNode(1.0f), floatConverter.convert(objectMapper, node, 1.0f));
        var booleanConverter = converters.createConverter(DataTypes.BOOLEAN().getLogicalType());
        assertEquals(BooleanNode.TRUE, booleanConverter.convert(objectMapper, node, true));
        var byteConverter = converters.createConverter(DataTypes.TINYINT().getLogicalType());
        assertEquals(new IntNode(1), byteConverter.convert(objectMapper, node, (byte) 1));
        var shortConverter = converters.createConverter(DataTypes.SMALLINT().getLogicalType());
        assertEquals(new ShortNode((short) 1), shortConverter.convert(objectMapper, node, (short) 1));
        var dateConverter = converters.createConverter(DataTypes.DATE().getLogicalType());
        assertEquals(new TextNode("1970-01-02"), dateConverter.convert(objectMapper, node, 1));
        var timeConverter = converters.createConverter(DataTypes.TIME().getLogicalType());
        assertEquals(new TextNode("01:00:00"), timeConverter.convert(objectMapper, node, 3600000));
        var timestampConverter = converters.createConverter(DataTypes.TIMESTAMP().getLogicalType());
        assertEquals(new TextNode("1970-01-01T00:00:00"), timestampConverter.convert(objectMapper, node, TimestampData.fromEpochMillis(0, 0)));
        var timestampWithLocalZoneConverter = converters.createConverter(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().getLogicalType());
        assertEquals(new TextNode("1970-01-01T00:00:00Z"), timestampWithLocalZoneConverter.convert(objectMapper, node, TimestampData.fromEpochMillis(0, 0)));
        var decimalConverter = converters.createConverter(DataTypes.DECIMAL(10, 2).getLogicalType());
        assertEquals(new DecimalNode(BigDecimal.ONE), decimalConverter.convert(objectMapper, node, DecimalData.fromBigDecimal(BigDecimal.ONE, 10, 2)));
        var arrayConverter = converters.createConverter(DataTypes.ARRAY(DataTypes.STRING()).getLogicalType());
        assertEquals(objectMapper.createArrayNode().add("test"),
                arrayConverter.convert(objectMapper, objectMapper.createArrayNode(), new GenericArrayData(new StringData[]{StringData.fromString("test")})));
        var mapConverter = converters.createConverter(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()).getLogicalType());
        assertEquals(objectMapper.createObjectNode().put("key", "value"),
                mapConverter.convert(objectMapper, node, new GenericMapData(Map.of(StringData.fromString("key"), StringData.fromString("value")))));
        var rowConverter = converters.createConverter(DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.STRING())).getLogicalType());
        GenericRowData row = new GenericRowData(1);
        row.setField(0, StringData.fromString("test"));
        node = objectMapper.createObjectNode();
        assertEquals(objectMapper.createObjectNode().put("f0", "test"),
                rowConverter.convert(objectMapper, node, row));
    }
}
