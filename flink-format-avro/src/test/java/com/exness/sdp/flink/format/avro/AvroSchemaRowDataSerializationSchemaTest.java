package com.exness.sdp.flink.format.avro;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import io.apicurio.registry.resolver.SchemaResolverConfig;
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

import static com.exness.sdp.flink.format.avro.AvroSchemaRowDataDeserializationSchemaTest.readResource;
import static io.apicurio.registry.serde.SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER;
import static io.apicurio.registry.serde.SerdeConfig.ENABLE_HEADERS;
import static io.apicurio.registry.serde.SerdeConfig.USE_ID;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class AvroSchemaRowDataSerializationSchemaTest {

    @Container
    private static final GenericContainer<?> schemaRegistry = new GenericContainer(
        DockerImageName.parse("quay.io/apicurio/apicurio-registry-mem:2.5.8.Final"))
        .withExposedPorts(8080)
        .waitingFor(Wait.forHttp("/apis/ccompat/v7"))
        .withStartupTimeout(Duration.ofMinutes(1));

    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

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

        AvroSchemaRowDataSerializationSchema serde = new AvroSchemaRowDataSerializationSchema(
            schema,  "schema", "namespace", config);
        serde.open(null);
        byte[] serialized = serde.serialize(rowData);
        byte[] expectedSchemaId = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 1};
        byte[] expectedValueBytes = new byte[]{2, 2, 49, 2, -128, -112, -76, -9, -44, 94, 2, 16, 112, 114, 111, 100, 117, 99, 101, 114};
        byte[] expectedBytes = new byte[expectedSchemaId.length + expectedValueBytes.length];
        System.arraycopy(expectedSchemaId, 0, expectedBytes, 0, expectedSchemaId.length);
        System.arraycopy(expectedValueBytes, 0, expectedBytes, expectedSchemaId.length, expectedValueBytes.length);
        assertArrayEquals(expectedBytes, serialized);
        assertEquals(new String(readResource("/schema/generated.avsc")), getArtifact("schema-value", 1));
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
        AvroSchemaRowDataSerializationSchema serde = new AvroSchemaRowDataSerializationSchema(
                schema, "schema", "namespace", config);
        serde.open(null);
        byte[] serialized = serde.serialize(rowData);
        byte[] expectedSchemaId = new byte[]{0, 0, 0, 0, 1};
        byte[] expectedValue = new byte[]{2, 2, 49, 2, -128, -112, -76, -9, -44, 94, 2, 16, 112, 114, 111, 100, 117, 99, 101, 114};
        byte[] expectedBytes = new byte[expectedSchemaId.length + expectedValue.length];
        System.arraycopy(expectedSchemaId, 0, expectedBytes, 0, expectedSchemaId.length);
        System.arraycopy(expectedValue, 0, expectedBytes, expectedSchemaId.length, expectedValue.length);
        assertArrayEquals(expectedBytes, serialized);
        assertEquals(new String(readResource("/schema/generated.avsc")), getArtifact("schema-confluent-value", 1));
    }

    @Test
    void testSerializationWithHeaders() {
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
        AvroSchemaRowDataSerializationSchema serde = new AvroSchemaRowDataSerializationSchema(
                schema, "schema", "namespace", config);
        serde.open(null);
        byte[] serialized = serde.serialize(rowData);
        byte[] expectedValue = new byte[]{2, 2, 49, 2, -128, -112, -76, -9, -44, 94, 2, 16, 112, 114, 111, 100, 117, 99, 101, 114};
        assertArrayEquals(expectedValue, serialized);
    }

    public static String getArtifact(String subject, int version) throws IOException, InterruptedException {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("%s/subjects/%s/versions/%s/schema", "http://"
                        + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8080) + "/apis/ccompat/v7", subject, version)))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .header("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Artifact registration failed, response was: " + response.body());
        return response.body();
    }
}
