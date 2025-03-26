package com.exness.sdp.flink.format.json.schema.serde;

import java.util.List;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import lombok.NoArgsConstructor;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;

@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class RowTypeToSchemaConverter {
    public static ObjectNode convertRowTypeToJsonSchema(RowType rowType, String schemaName) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode schema = convertLogicalTypeToJsonSchema(rowType, mapper);
        if (schema instanceof ObjectNode objectNode) {
            objectNode.put("$schema", "http://json-schema.org/draft-04/schema#");
            objectNode.put("title", schemaName);
            return objectNode;
        }
        throw new IllegalArgumentException("Failed to convert RowType to JSON Schema - output is not an object node");
    }

    private static JsonNode convertLogicalTypeToJsonSchema(LogicalType logicalType, ObjectMapper mapper) {
        var fieldSchema = mapper.createObjectNode();
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                fieldSchema.put("type", "boolean");
                break;
            case INTEGER, BIGINT, SMALLINT, TINYINT:
                fieldSchema.put("type", "integer");
                break;
            case FLOAT, DOUBLE, DECIMAL:
                fieldSchema.put("type", "number");
                break;
            case MAP:
                fieldSchema.put("type", "object");
                if (!(logicalType.getChildren().get(0).getTypeRoot().equals(VARCHAR)
                    || logicalType.getChildren().get(0).getTypeRoot().equals(CHAR))) {
                    throw new IllegalArgumentException("Only String keys are supported in Map type for JSON Schema");
                }
                fieldSchema.set("additionalProperties", convertLogicalTypeToJsonSchema(logicalType.getChildren().get(1), mapper));
                break;
            case ROW:
                fieldSchema.put("type", "object");
                List<RowType.RowField> fields = ((RowType) logicalType).getFields();
                ObjectNode properties = mapper.createObjectNode();
                for (RowType.RowField field : fields) {
                    JsonNode schema = convertLogicalTypeToJsonSchema(field.getType(), mapper);
                    properties.set(field.getName(), schema);
                }
                fieldSchema.set("properties", properties);
                break;
            case DISTINCT_TYPE, STRUCTURED_TYPE, NULL, RAW, SYMBOL, BINARY, VARBINARY, VARCHAR, CHAR, DATE,
                    TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE,
                    TIMESTAMP_WITH_LOCAL_TIME_ZONE, INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME:
                fieldSchema.put("type", "string");
                break;
            case ARRAY:
                fieldSchema.put("type", "array");
                LogicalType childType = logicalType.getChildren().get(0);
                JsonNode items = convertLogicalTypeToJsonSchema(childType, mapper);
                fieldSchema.set("items", items);
                break;
                // Multiset type has integer key type, which is not supported by JSON Schema
            case MULTISET:
            default:
                throw new IllegalArgumentException("Unsupported type: " + logicalType);
        }
        return fieldSchema;
    }
}
