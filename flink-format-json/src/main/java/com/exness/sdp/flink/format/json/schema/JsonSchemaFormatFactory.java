package com.exness.sdp.flink.format.json.schema;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonFormatOptionsUtil;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import lombok.SneakyThrows;

import static com.exness.sdp.flink.common.serde.SerdeUtils.toUpperCamelCase;
import static com.exness.sdp.flink.format.json.schema.JsonSchemaFormatOptions.SCHEMA_NAME;
import static com.exness.sdp.flink.format.json.schema.JsonSchemaFormatOptions.SCHEMA_REGISTRY_AUTO_REGISTER_ARTIFACT;
import static com.exness.sdp.flink.format.json.schema.JsonSchemaFormatOptions.SCHEMA_REGISTRY_ENABLE_CONFLUENT_ID_HANDLER;
import static com.exness.sdp.flink.format.json.schema.JsonSchemaFormatOptions.SCHEMA_REGISTRY_ENABLE_HEADERS;
import static com.exness.sdp.flink.format.json.schema.JsonSchemaFormatOptions.SCHEMA_REGISTRY_SUBJECT;
import static com.exness.sdp.flink.format.json.schema.JsonSchemaFormatOptions.SCHEMA_REGISTRY_URL;
import static com.exness.sdp.flink.format.json.schema.JsonSchemaFormatOptions.SCHEMA_REGISTRY_USE_ID;
import static io.apicurio.registry.serde.SerdeConfig.EXPLICIT_ARTIFACT_ID;
import static org.apache.flink.formats.json.JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;
import static org.apache.flink.formats.json.JsonFormatOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.flink.formats.json.JsonFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.json.JsonFormatOptions.MAP_NULL_KEY_LITERAL;
import static org.apache.flink.formats.json.JsonFormatOptions.MAP_NULL_KEY_MODE;
import static org.apache.flink.formats.json.JsonFormatOptions.TIMESTAMP_FORMAT;

public class JsonSchemaFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {
    public static final String IDENTIFIER = "json-schema";


    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        JsonFormatOptionsUtil.validateDecodingFormatOptions(formatOptions);

        final boolean failOnMissingField = formatOptions.get(FAIL_ON_MISSING_FIELD);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        TimestampFormat timestampOption = JsonFormatOptionsUtil.getTimestampFormat(formatOptions);

        final Map<String, Object> registryConfig =
                JsonSchemaFormatOptions.getRegistryProperties(context.getCatalogTable().getOptions(), formatOptions);

        return new ProjectableDecodingFormat<>() {
            @SneakyThrows
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType physicalDataType,
                    int[][] projections) {
                final DataType producedDataType =
                        Projection.of(projections).project(physicalDataType);
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDataType);
                return new JsonSchemaRowDataDeserializationSchema(
                        rowType,
                        rowDataTypeInfo,
                        failOnMissingField,
                        ignoreParseErrors,
                        timestampOption,
                        registryConfig);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        JsonFormatOptionsUtil.validateEncodingFormatOptions(formatOptions);

        TimestampFormat timestampOption = JsonFormatOptionsUtil.getTimestampFormat(formatOptions);
        JsonFormatOptions.MapNullKeyMode mapNullKeyMode =
                JsonFormatOptionsUtil.getMapNullKeyMode(formatOptions);
        String mapNullKeyLiteral = formatOptions.get(MAP_NULL_KEY_LITERAL);

        final boolean encodeDecimalAsPlainNumber =
                formatOptions.get(ENCODE_DECIMAL_AS_PLAIN_NUMBER);

        final Map<String, Object> registryConfig =
                JsonSchemaFormatOptions.getRegistryProperties(context.getCatalogTable().getOptions(), formatOptions);

        validateSubjectName(formatOptions, registryConfig);

        String schemaRegistrySubject = (String) registryConfig.get(EXPLICIT_ARTIFACT_ID);

        if (schemaRegistrySubject == null) {
            schemaRegistrySubject = formatOptions.get(SCHEMA_REGISTRY_SUBJECT);
            registryConfig.put(EXPLICIT_ARTIFACT_ID, schemaRegistrySubject);
        }


        final String schemaName = formatOptions.getOptional(SCHEMA_NAME).orElse(toUpperCamelCase(schemaRegistrySubject));

        return new EncodingFormat<>() {
            @SneakyThrows
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                return new JsonSchemaRowDataSerializationSchema(
                        rowType,
                        timestampOption,
                        mapNullKeyMode,
                        mapNullKeyLiteral,
                        encodeDecimalAsPlainNumber,
                        schemaName,
                        registryConfig);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    public static void validateSubjectName(ReadableConfig tableOptions, Map<String, Object> registryConfig) {
        if (registryConfig.containsKey(EXPLICIT_ARTIFACT_ID) && !(registryConfig.get(EXPLICIT_ARTIFACT_ID) instanceof String)) {
            throw new ValidationException(
                    String.format(
                            "Invalid value '%s' for option %s. Expected a string.",
                            registryConfig.get(EXPLICIT_ARTIFACT_ID), EXPLICIT_ARTIFACT_ID));
        }
        if (tableOptions.getOptional(SCHEMA_REGISTRY_SUBJECT).isEmpty() && !registryConfig.containsKey(EXPLICIT_ARTIFACT_ID)) {
            throw new ValidationException(
                    String.format(
                            "Either %s or %s is required but not set.",
                            SCHEMA_REGISTRY_SUBJECT.key(), EXPLICIT_ARTIFACT_ID));
        }
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>(JsonSchemaFormatOptions.getApicurioRegistryOptions());
        options.add(FAIL_ON_MISSING_FIELD);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(TIMESTAMP_FORMAT);
        options.add(MAP_NULL_KEY_MODE);
        options.add(MAP_NULL_KEY_LITERAL);
        options.add(ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        options.add(SCHEMA_NAME);
        options.add(SCHEMA_REGISTRY_SUBJECT);
        options.add(SCHEMA_REGISTRY_URL);
        options.add(SCHEMA_REGISTRY_ENABLE_CONFLUENT_ID_HANDLER);
        options.add(SCHEMA_REGISTRY_ENABLE_HEADERS);
        options.add(SCHEMA_REGISTRY_AUTO_REGISTER_ARTIFACT);
        options.add(SCHEMA_REGISTRY_USE_ID);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TIMESTAMP_FORMAT);
        options.add(MAP_NULL_KEY_MODE);
        options.add(MAP_NULL_KEY_LITERAL);
        options.add(ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        return options;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
