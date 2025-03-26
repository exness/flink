package com.exness.sdp.flink.format.json.schema;


import java.io.IOException;
import java.io.Serial;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaDeserializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import com.exness.sdp.flink.format.json.util.JsonToRowDataConverters;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class JsonSchemaRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    @Serial
    private static final long serialVersionUID = 1L;

    private final boolean failOnMissingField;

    private final boolean ignoreParseErrors;

    private final TypeInformation<RowData> resultTypeInfo;

    private final JsonToRowDataConverters.JsonToRowDataConverter runtimeConverter;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final TimestampFormat timestampFormat;
    private transient JsonSchemaKafkaDeserializer<JsonNode> deserializer;

    @SuppressWarnings("java:S1948")
    private final Map<String, Object> config;

    public JsonSchemaRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat,
            Map<String, Object> config) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.runtimeConverter =
                new JsonToRowDataConverters(failOnMissingField, ignoreParseErrors, timestampFormat)
                        .createConverter(checkNotNull(rowType));
        this.timestampFormat = timestampFormat;
        boolean hasDecimalType =
                LogicalTypeChecks.hasNested(rowType, DecimalType.class::isInstance);
        if (hasDecimalType) {
            objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
        this.config = config;
    }

    @Override
    public void open(InitializationContext context) {
        deserializer = new JsonSchemaKafkaDeserializer<>();
        deserializer.configure(config, false);
        deserializer.setObjectMapper(objectMapper);
    }

    @Override
    @SuppressWarnings("java:S1181")
    public RowData deserialize(@Nullable byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            return convertToRowData(deserializeToJsonNode(message));
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(
                    format("Failed to deserialize JSON '%s'.", new String(message)), t);
        }
    }

    public JsonNode deserializeToJsonNode(byte[] message) {
        return deserializer.deserialize(null, message);
    }

    public RowData convertToRowData(JsonNode message) {
        return (RowData) runtimeConverter.convert(message);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonSchemaRowDataDeserializationSchema that = (JsonSchemaRowDataDeserializationSchema) o;
        return failOnMissingField == that.failOnMissingField
                && ignoreParseErrors == that.ignoreParseErrors
                && resultTypeInfo.equals(that.resultTypeInfo)
                && timestampFormat.equals(that.timestampFormat)
                && config.equals(that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failOnMissingField, ignoreParseErrors, resultTypeInfo, timestampFormat, config);
    }
}
