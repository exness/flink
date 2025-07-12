package com.exness.sdp.flink.format.json.schema;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import io.apicurio.registry.resolver.SchemaResolverConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static io.apicurio.registry.serde.SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER;
import static io.apicurio.registry.serde.SerdeConfig.ENABLE_HEADERS;
import static io.apicurio.registry.serde.SerdeConfig.USE_ID;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@Testcontainers
class JsonSchemaRowDataSerializationSchemaTest {

    @Container
    private static final GenericContainer<?> schemaRegistry = new GenericContainer(
        DockerImageName.parse("quay.io/apicurio/apicurio-registry-mem:2.5.8.Final"))
        .withExposedPorts(8080)
        .waitingFor(Wait.forHttp("/apis/ccompat/v7"))
        .withStartupTimeout(Duration.ofMinutes(1));


    @Test
    void testSerialization() throws Exception {
        GenericRowData rowData = new GenericRowData(3);
        rowData.setField(0, StringData.fromString("1"));
        rowData.setField(1, 1626307200000L);
        rowData.setField(2, StringData.fromString("producer"));

        DataType dataType =
            ROW(
                FIELD("id", STRING()),
                FIELD("created_at", BIGINT()),
                FIELD("producer", STRING())
            );
        RowType schema = (RowType) dataType.getLogicalType();
        Map<String, Object> config = Map.of(SchemaResolverConfig.REGISTRY_URL,
            "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8080),
            SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, "schema-value",
            SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE,
                ENABLE_CONFLUENT_ID_HANDLER, Boolean.FALSE,
                ENABLE_HEADERS, Boolean.FALSE);

        JsonSchemaRowDataSerializationSchema serde = new JsonSchemaRowDataSerializationSchema(
            schema, TimestampFormat.ISO_8601, JsonFormatOptions.MapNullKeyMode.LITERAL, "null", false, "schema", config);
        serde.open(null);
        byte[] serialized = serde.serialize(rowData);
        byte[] expectedSchemaId = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 1};
        byte[] expectedValue = "{\"id\":\"1\",\"created_at\":1626307200000,\"producer\":\"producer\"}".getBytes(StandardCharsets.UTF_8);
        byte[] expectedBytes = new byte[expectedSchemaId.length + expectedValue.length];
        System.arraycopy(expectedSchemaId, 0, expectedBytes, 0, expectedSchemaId.length);
        System.arraycopy(expectedValue, 0, expectedBytes, expectedSchemaId.length, expectedValue.length);
        assertArrayEquals(expectedBytes, serialized);
    }

    @Test
    void testSerializationAsConfluent() throws Exception {
        GenericRowData rowData = new GenericRowData(3);
        rowData.setField(0, StringData.fromString("1"));
        rowData.setField(1, 1626307200000L);
        rowData.setField(2, StringData.fromString("producer"));

        DataType dataType =
                ROW(
                        FIELD("id", STRING()),
                        FIELD("created_at", BIGINT()),
                        FIELD("producer", STRING())
                );
        RowType schema = (RowType) dataType.getLogicalType();
        Map<String, Object> config = Map.of(SchemaResolverConfig.REGISTRY_URL,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8080),
                SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, "schema-confluent-value",
                SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE,
                ENABLE_HEADERS, Boolean.FALSE,
                ENABLE_CONFLUENT_ID_HANDLER, Boolean.TRUE,
                USE_ID, "contentId");
        JsonSchemaRowDataSerializationSchema serde = new JsonSchemaRowDataSerializationSchema(
                schema, TimestampFormat.ISO_8601, JsonFormatOptions.MapNullKeyMode.LITERAL, "null", false, "schema", config);
        serde.open(null);
        byte[] serialized = serde.serialize(rowData);
        byte[] expectedSchemaId = new byte[]{0, 0, 0, 0, 1};
        byte[] expectedValue = "{\"id\":\"1\",\"created_at\":1626307200000,\"producer\":\"producer\"}".getBytes(StandardCharsets.UTF_8);
        byte[] expectedBytes = new byte[expectedSchemaId.length + expectedValue.length];
        System.arraycopy(expectedSchemaId, 0, expectedBytes, 0, expectedSchemaId.length);
        System.arraycopy(expectedValue, 0, expectedBytes, expectedSchemaId.length, expectedValue.length);
        assertArrayEquals(expectedBytes, serialized);
    }

    @Test
    void testSerializationWithHeaders() throws Exception {
        GenericRowData rowData = new GenericRowData(3);
        rowData.setField(0, StringData.fromString("1"));
        rowData.setField(1, 1626307200000L);
        rowData.setField(2, StringData.fromString("producer"));

        DataType dataType =
                ROW(
                        FIELD("id", STRING()),
                        FIELD("created_at", BIGINT()),
                        FIELD("producer", STRING())
                );
        RowType schema = (RowType) dataType.getLogicalType();
        Map<String, Object> config = Map.of(SchemaResolverConfig.REGISTRY_URL,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8080),
                SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, "schema-confluent-value",
                SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE,
                ENABLE_HEADERS, Boolean.TRUE);
        JsonSchemaRowDataSerializationSchema serde = new JsonSchemaRowDataSerializationSchema(
                schema, TimestampFormat.ISO_8601, JsonFormatOptions.MapNullKeyMode.LITERAL, "null", false, "schema", config);
        serde.open(null);
        byte[] serialized = serde.serialize(rowData);
        byte[] expectedValue = "{\"id\":\"1\",\"created_at\":1626307200000,\"producer\":\"producer\"}".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expectedValue, serialized);
    }
}
