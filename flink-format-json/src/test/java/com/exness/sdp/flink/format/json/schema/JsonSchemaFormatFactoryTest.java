package com.exness.sdp.flink.format.json.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.junit.jupiter.api.Test;


import static com.exness.sdp.flink.format.json.schema.JsonSchemaFormatFactory.validateSubjectName;
import static com.exness.sdp.flink.format.json.schema.JsonSchemaFormatOptions.SCHEMA_REGISTRY_SUBJECT;
import static com.exness.sdp.flink.format.json.schema.JsonSchemaFormatOptions.SCHEMA_REGISTRY_URL;
import static io.apicurio.registry.serde.SerdeConfig.EXPLICIT_ARTIFACT_ID;
import static io.apicurio.registry.serde.SerdeConfig.VALIDATION_ENABLED;
import static org.apache.flink.table.factories.utils.FactoryMocks.PHYSICAL_DATA_TYPE;
import static org.apache.flink.table.factories.utils.FactoryMocks.PHYSICAL_TYPE;
import static org.apache.flink.table.factories.utils.FactoryMocks.SCHEMA;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("java:S5778")
class JsonSchemaFormatFactoryTest {

    @Test
    void testSeDeSchema() {
        final Map<String, String> tableOptions = getAllOptions();

        testSchemaDeserializationSchema(tableOptions);
    }

    @Test
    void testFailOnMissingField() {
        final Map<String, String> tableOptions =
                getModifyOptions(options -> options.put("json-schema.fail-on-missing-field", "true"));

        assertThrows(ValidationException.class, () ->
                createTableSource(tableOptions)
                        .valueFormat
                        .createRuntimeDecoder(
                                ScanRuntimeProviderContext.INSTANCE,
                                SCHEMA.toPhysicalRowDataType()));
    }

    @Test
    void testInvalidOptionForIgnoreParseErrors() {
        final Map<String, String> tableOptions =
                getModifyOptions(options -> options.put("json-schema.ignore-parse-errors", "abc"));

        assertThrows(ValidationException.class, () ->
                createTableSource(tableOptions)
                        .valueFormat
                        .createRuntimeDecoder(
                                ScanRuntimeProviderContext.INSTANCE,
                                SCHEMA.toPhysicalRowDataType()));
    }

    @Test
    void testInvalidOptionForTimestampFormat() {
        final Map<String, String> tableOptions =
                getModifyOptions(options -> options.put("json-schema.timestamp-format.standard", "test"));

        assertThrows(ValidationException.class, () ->
                createTableSource(tableOptions)
                        .valueFormat
                        .createRuntimeDecoder(
                                ScanRuntimeProviderContext.INSTANCE,
                                SCHEMA.toPhysicalRowDataType()));
    }

    @Test
    void testLowerCaseOptionForTimestampFormat() {
        final Map<String, String> tableOptions =
                getModifyOptions(
                        options -> options.put("json-schema.timestamp-format.standard", "iso-8601"));

        assertThrows(ValidationException.class, () ->
                createTableSource(tableOptions)
                        .valueFormat
                        .createRuntimeDecoder(
                                ScanRuntimeProviderContext.INSTANCE,
                                SCHEMA.toPhysicalRowDataType()));
    }

    @Test
    void testInvalidOptionForMapNullKeyMode() {
        final Map<String, String> tableOptions =
                getModifyOptions(options -> options.put("json-schema.map-null-key.mode", "invalid"));

        assertThrows(ValidationException.class, () ->
                createTableSink(tableOptions)
                        .valueFormat
                        .createRuntimeEncoder(
                                new SinkRuntimeProviderContext(false), PHYSICAL_DATA_TYPE));
    }

    @Test
    void testValidSinkOptions() {
        final Map<String, String> tableOptions = getAllOptions();
        assertDoesNotThrow(() ->
                createTableSink(tableOptions)
                        .valueFormat
                        .createRuntimeEncoder(
                                new SinkRuntimeProviderContext(false), PHYSICAL_DATA_TYPE));
    }

    @Test
    void testValidateSubjectName() {
        Configuration configuration = new Configuration();
        configuration.setString(SCHEMA_REGISTRY_SUBJECT.key(), "subject-value");
        assertDoesNotThrow(() -> validateSubjectName(configuration, Map.of()));
        assertDoesNotThrow(() -> validateSubjectName(new Configuration(), Map.of(EXPLICIT_ARTIFACT_ID, "subject-value")));
        assertThrows(ValidationException.class, () -> validateSubjectName(new Configuration(), Map.of()));
        assertThrows(ValidationException.class, () -> validateSubjectName(new Configuration(), Map.of(EXPLICIT_ARTIFACT_ID, 1)));
    }

    @Test
    void testLowerCaseOptionForMapNullKeyMode() {
        final Map<String, String> tableOptions =
                getModifyOptions(options -> options.put("json-schema.map-null-key.mode", "fail"));

        testSchemaDeserializationSchema(tableOptions);
    }

    private void testSchemaDeserializationSchema(Map<String, String> options) {
        final JsonSchemaRowDataDeserializationSchema expectedDeser =
                new JsonSchemaRowDataDeserializationSchema(
                        PHYSICAL_TYPE,
                        InternalTypeInfo.of(PHYSICAL_TYPE),
                        false,
                        true,
                        TimestampFormat.ISO_8601,
                        Map.of("apicurio.registry.url", "http://localhost:8080",
                                "apicurio.registry.serde.validation-enabled", "true",
                                "apicurio.registry.headers.enabled", true,
                                "apicurio.registry.auto-register", true,
                                "apicurio.registry.use-id", "contentId",
                                "apicurio.registry.as-confluent", true)
                );

        DeserializationSchema<RowData> actualDeser =
                createTableSource(options)
                        .valueFormat
                        .createRuntimeDecoder(
                                ScanRuntimeProviderContext.INSTANCE,
                                SCHEMA.toPhysicalRowDataType());

        assertEquals(actualDeser, expectedDeser);
    }

    private TestDynamicTableFactory.DynamicTableSinkMock createTableSink(
            Map<String, String> options) {
        final DynamicTableSink actualSink = FactoryMocks.createTableSink(SCHEMA, options);
        assertInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class, actualSink);

        return (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;
    }

    private TestDynamicTableFactory.DynamicTableSourceMock createTableSource(
            Map<String, String> options) {
        final DynamicTableSource actualSource = FactoryMocks.createTableSource(SCHEMA, options);
        assertInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class, actualSource);

        return (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;
    }

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private Map<String, String> getModifyOptions(Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getAllOptions();
        optionModifier.accept(options);
        return options;
    }

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", JsonSchemaFormatFactory.IDENTIFIER);
        options.put("json-schema.fail-on-missing-field", "false");
        options.put("json-schema.ignore-parse-errors", "true");
        options.put("json-schema.timestamp-format.standard", "ISO-8601");
        options.put("json-schema.map-null-key.mode", "LITERAL");
        options.put("json-schema.map-null-key.literal", "null");
        options.put("json-schema.encode.decimal-as-plain-number", "true");
        options.put("json-schema." + SCHEMA_REGISTRY_URL.key(), "http://localhost:8080");
        options.put("json-schema." + VALIDATION_ENABLED, "true");
        options.put("json-schema." + SCHEMA_REGISTRY_SUBJECT.key(), "schema-value");
        return options;
    }
}
