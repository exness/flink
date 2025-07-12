package com.exness.sdp.flink.format.json.schema.serde;

import java.io.Serializable;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.apicurio.registry.resolver.ParsedSchema;
import io.apicurio.registry.resolver.ParsedSchemaImpl;
import io.apicurio.registry.resolver.data.Record;
import io.apicurio.registry.serde.jsonschema.JsonSchema;
import io.apicurio.registry.serde.jsonschema.JsonSchemaParser;
import org.apache.flink.table.types.logical.RowType;

public class FlinkJsonSchemaParser<T> extends JsonSchemaParser<T> implements Serializable {
    private final ObjectNode parsedSchema;

    public FlinkJsonSchemaParser(RowType rowType, String schemaName) {
        super();
        this.parsedSchema = RowTypeToSchemaConverter.convertRowTypeToJsonSchema(rowType, schemaName);
    }

    @Override
    /* This behaviour is inherited from Apicurio JsonSchemaParser */
    @SuppressWarnings({"S3740", "unchecked", "rawtypes"})
    public ParsedSchema<JsonSchema> getSchemaFromData(Record<T> data) {
        return (new ParsedSchemaImpl())
                .setParsedSchema(new JsonSchema(parsedSchema))
                .setRawSchema(parsedSchema.toString().getBytes());
    }

    @Override
    public ParsedSchema<JsonSchema> getSchemaFromData(Record<T> data, boolean dereference) {
        return getSchemaFromData(data);
    }

    @Override
    public boolean supportsExtractSchemaFromData() {
        return true;
    }
}
