package com.exness.sdp.flink.format.json.schema.serde;

import io.apicurio.registry.resolver.SchemaParser;
import io.apicurio.registry.serde.jsonschema.JsonSchema;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.serialization.Serializer;

public class FlinkJsonSchemaKafkaSerializer<T> extends JsonSchemaKafkaSerializer<T> implements Serializer<T> {

    private final FlinkJsonSchemaParser<T> parser;

    public FlinkJsonSchemaKafkaSerializer(RowType rowType, String schemaName) {
        super();
        parser = new FlinkJsonSchemaParser<>(rowType, schemaName);
    }

    @Override
    public SchemaParser<JsonSchema, T> schemaParser() {
        return parser;
    }

}