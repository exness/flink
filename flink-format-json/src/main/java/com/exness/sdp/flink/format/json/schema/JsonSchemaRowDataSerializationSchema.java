package com.exness.sdp.flink.format.json.schema;

import java.io.Serial;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import com.exness.sdp.flink.common.serde.kafka.SerializerWithHeaders;
import com.exness.sdp.flink.format.json.schema.serde.FlinkJsonSchemaKafkaSerializer;
import com.exness.sdp.flink.format.json.util.RowDataToJsonConverters;

import static java.lang.String.format;

public class JsonSchemaRowDataSerializationSchema implements SerializationSchema<RowData>, SerializerWithHeaders {
    @Serial
    private static final long serialVersionUID = 1L;

    private transient JsonSchemaKafkaSerializer<JsonNode> serializer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RowDataToJsonConverters.RowDataToJsonConverter runtimeConverter;
    @SuppressWarnings("java:S1450")
    private transient ObjectNode node;
    private final RowType rowType;
    private final TimestampFormat timestampFormat;
    private final JsonFormatOptions.MapNullKeyMode mapNullKeyMode;
    private final boolean encodeDecimalAsPlainNumber;
    private final String mapNullKeyLiteral;
    @SuppressWarnings("java:S1948")
    private final Map<String, Object> config;
    private final String schemaName;

    public JsonSchemaRowDataSerializationSchema(
        RowType rowType,
        TimestampFormat timestampFormat,
        JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
        String mapNullKeyLiteral,
        boolean encodeDecimalAsPlainNumber,
        String schemaName,
        Map<String, Object> config) {
        this.rowType = rowType;
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
        this.encodeDecimalAsPlainNumber = encodeDecimalAsPlainNumber;
        this.config = config;
        this.schemaName = schemaName;
        this.objectMapper.configure(
            JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, encodeDecimalAsPlainNumber);
        this.runtimeConverter =
            new RowDataToJsonConverters(timestampFormat, mapNullKeyMode, mapNullKeyLiteral, true)
                .createConverter(rowType);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.serializer = new FlinkJsonSchemaKafkaSerializer<>(rowType, schemaName);
        this.serializer.setObjectMapper(objectMapper);
        this.serializer.configure(config, false);
    }

    public byte[] serialize(RowData row, Headers headers) {
        node = objectMapper.createObjectNode();
        try {
            return switch (row.getRowKind()) {
                case INSERT, UPDATE_AFTER -> {
                    runtimeConverter.convert(objectMapper, node, row);
                    yield serializer.serialize(null, headers, node);
                }
                case UPDATE_BEFORE, DELETE -> null;
            };
        } catch (Exception t) {
            throw new IllegalStateException(format("Could not serialize row '%s'.", row), t);
        }
    }

    @Override
    public byte[] serialize(RowData row) {
        return serialize(row, new RecordHeaders());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonSchemaRowDataSerializationSchema that = (JsonSchemaRowDataSerializationSchema) o;
        return rowType.equals(that.rowType)
            && timestampFormat.equals(that.timestampFormat)
            && mapNullKeyMode.equals(that.mapNullKeyMode)
            && mapNullKeyLiteral.equals(that.mapNullKeyLiteral)
            && encodeDecimalAsPlainNumber == that.encodeDecimalAsPlainNumber
            && config.equals(that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            rowType,
            timestampFormat,
            mapNullKeyMode,
            mapNullKeyLiteral,
            encodeDecimalAsPlainNumber,
            config);
    }
}
