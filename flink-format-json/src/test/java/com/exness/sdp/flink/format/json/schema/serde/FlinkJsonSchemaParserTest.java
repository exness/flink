package com.exness.sdp.flink.format.json.schema.serde;

import com.fasterxml.jackson.databind.JsonNode;
import io.apicurio.registry.resolver.ParsedSchema;
import io.apicurio.registry.resolver.ParsedSchemaImpl;
import io.apicurio.registry.resolver.data.Metadata;
import io.apicurio.registry.resolver.data.Record;
import io.apicurio.registry.serde.jsonschema.JsonSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class FlinkJsonSchemaParserTest {
    @Test
    void getSchemaFromDataTest() {
        DataType dataType =
                ROW(
                        FIELD("id", STRING()),
                        FIELD("created_at", BIGINT()),
                        FIELD("producer", STRING())
                        );

        FlinkJsonSchemaParser<JsonNode> flinkJsonSchemaParser = new FlinkJsonSchemaParser<>((RowType) dataType.getLogicalType(), "schema");
        ParsedSchema<JsonSchema> expected = flinkJsonSchemaParser.getSchemaFromData(new Record() {
            @Override
            public Metadata metadata() {
                return null;
            }

            @Override
            public Object payload() {
                return null;
            }
        });

        var parsedSchema = new ParsedSchemaImpl();
        parsedSchema.setParsedSchema(new JsonSchema(RowTypeToSchemaConverter.convertRowTypeToJsonSchema((RowType) dataType.getLogicalType(), "schema")));
        assertEquals(expected.getParsedSchema(), parsedSchema.getParsedSchema());
    }

    @Test
    void supportsExtractSchemaFromDataTest() {
        DataType dataType =
                ROW(
                        FIELD("id", STRING())
                );

        assertTrue(new FlinkJsonSchemaParser<>((RowType) dataType.getLogicalType(), "test").supportsExtractSchemaFromData());
    }
}
