package com.exness.sdp.flink.format.json.schema.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.exness.sdp.flink.format.json.schema.JsonSchemaRowDataDeserializationSchemaTest.readResource;
import static com.exness.sdp.flink.format.json.schema.serde.RowTypeToSchemaConverter.convertRowTypeToJsonSchema;
import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RowTypeToSchemaConverterTest {

    @Test
    void convertRowTypeToJsonSchemaTest() throws IOException {
        DataType dataType =
            ROW(
                FIELD("id", STRING()),
                FIELD("created_at", BIGINT()),
                FIELD("producer", STRING()),
                FIELD("body", ROW(
                    FIELD("1.0.0", ROW(
                        FIELD("meta", ROW(
                            FIELD("notification_id", STRING()),
                            FIELD("trace_id", STRING())
                        )),
                        FIELD("data", ROW(
                            FIELD("is_unique", BOOLEAN()),
                            FIELD("is_repetitive", BOOLEAN()),
                            FIELD("metadata", ROW(
                                FIELD("commission_amount", INT()),
                                FIELD("number_of_new_registrations", INT())
                            )),
                            FIELD("user_id", STRING()),
                            FIELD("template_id", STRING()),
                            FIELD("type", STRING()),
                            FIELD("displayed_at", STRING()),
                            FIELD("expired_at", STRING()),
                            FIELD("recipients", ARRAY(STRING())),
                            FIELD("sample_map", MAP(
                                STRING(),
                                ROW(
                                    FIELD("key", STRING()),
                                    FIELD("value", INT())
                                ))),
                            FIELD("sample_array", ARRAY(INT()))
                        ))
                    )),
                    FIELD("iss", STRING())
                )));
        String expected = readResource("/schemas/schema-2.json");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expectedJson = mapper.readTree(expected);
        RowType schema = (RowType) dataType.getLogicalType();
        JsonNode jsonSchema = convertRowTypeToJsonSchema(schema, "test");
        assertEquals(expectedJson, jsonSchema);
    }

    @Test
    void convertRowTypeToJsonSchemaFailTest() {
        DataType dataType =
            ROW(
                FIELD("id", MULTISET(STRING()))
            );
        RowType schema = (RowType) dataType.getLogicalType();
        assertThrows(IllegalArgumentException.class, () -> convertRowTypeToJsonSchema(schema, "test"));

        DataType dataType2 =
            ROW(
                FIELD("id", MAP(INT(), STRING()))
            );
        RowType schema2 = (RowType) dataType2.getLogicalType();
        assertThrows(IllegalArgumentException.class, () -> convertRowTypeToJsonSchema(schema2, "test"));
    }
}
