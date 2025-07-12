package com.exness.sdp.flink.format.json.schema;


import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.apicurio.registry.resolver.SchemaResolverConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;


import static io.apicurio.registry.serde.SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER;
import static io.apicurio.registry.serde.SerdeConfig.USE_ID;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class JsonSchemaRowDataDeserializationSchemaTest {

    @Container
    private static final GenericContainer<?> schemaRegistry = new GenericContainer(
            DockerImageName.parse("quay.io/apicurio/apicurio-registry-mem:2.5.8.Final"))
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/apis/ccompat/v7"))
            .withStartupTimeout(Duration.ofMinutes(1));

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @Test
    void testApicurioDeserialization() throws IOException, InterruptedException {
        GenericRowData expectedRowData = new GenericRowData(3);
        expectedRowData.setField(0, StringData.fromString("1"));
        expectedRowData.setField(1, 1626307200000L);
        expectedRowData.setField(2, StringData.fromString("producer"));

        DataType dataType =
                ROW(
                        FIELD("id", STRING()),
                        FIELD("created_at", BIGINT()),
                        FIELD("producer", STRING())
                );
        RowType schema = (RowType) dataType.getLogicalType();
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(schema);
        Map<String, Object> config = Map.of(SchemaResolverConfig.REGISTRY_URL,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8080),
                SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, "schema-value",
                SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE);

        JsonSchemaRowDataDeserializationSchema serde = new JsonSchemaRowDataDeserializationSchema(
                schema,
                resultTypeInfo,
                false,
                false,
                TimestampFormat.ISO_8601,
                config);
        serde.open(null);

        String schemaString = readResource("/schemas/schema-1.json");
        int actualSchemaId = registerArtifact("schema-value", schemaString);

        byte[] schemaId = ByteBuffer.allocate(9).putInt(5, actualSchemaId).array();
        byte[] value = "{\"id\":\"1\",\"created_at\":1626307200000,\"producer\":\"producer\"}".getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[schemaId.length + value.length];
        System.arraycopy(schemaId, 0, bytes, 0, schemaId.length);
        System.arraycopy(value, 0, bytes, schemaId.length, value.length);
        RowData actualDeserialized = serde.deserialize(bytes);

        assertEquals(expectedRowData, actualDeserialized);
    }

    @Test
    void testConfluentDeserialization() throws IOException, InterruptedException {
        GenericRowData expectedRowData = new GenericRowData(3);
        expectedRowData.setField(0, StringData.fromString("1"));
        expectedRowData.setField(1, 1626307200000L);
        expectedRowData.setField(2, StringData.fromString("producer"));

        DataType dataType =
                ROW(
                        FIELD("id", STRING()),
                        FIELD("created_at", BIGINT()),
                        FIELD("producer", STRING())
                );
        RowType schema = (RowType) dataType.getLogicalType();
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(schema);
        Map<String, Object> config = Map.of(SchemaResolverConfig.REGISTRY_URL,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8080),
                SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, "schema-value",
                SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE,
                ENABLE_CONFLUENT_ID_HANDLER, Boolean.TRUE,
                USE_ID, "contentId");

        JsonSchemaRowDataDeserializationSchema serde = new JsonSchemaRowDataDeserializationSchema(
                schema,
                resultTypeInfo,
                false,
                false,
                TimestampFormat.ISO_8601,
                config);
        serde.open(null);

        String schemaString = readResource("/schemas/schema-1.json");
        int actualSchemaId = registerArtifact("schema-value", schemaString);

        byte[] schemaId = ByteBuffer.allocate(5).putInt(1, actualSchemaId).array();
        byte[] value = "{\"id\":\"1\",\"created_at\":1626307200000,\"producer\":\"producer\"}".getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[schemaId.length + value.length];
        System.arraycopy(schemaId, 0, bytes, 0, schemaId.length);
        System.arraycopy(value, 0, bytes, schemaId.length, value.length);
        RowData actualDeserialized = serde.deserialize(bytes);

        assertEquals(expectedRowData, actualDeserialized);
    }

    private static int registerArtifact(String subject, String schema) throws IOException, InterruptedException {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("schemaType", "JSON");
        payload.put("schema", schema);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("%s/subjects/%s/versions", "http://"
                        + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8080) + "/apis/ccompat/v7", subject)))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .header("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Artifact registration failed, response was: " + response.body());
        JsonNode node = mapper.readTree(response.body());
        return node.get("id").asInt();
    }

    public static String readResource(String fileName) throws IOException {
        return new String(new BufferedInputStream(
                Objects.requireNonNull(JsonSchemaRowDataDeserializationSchemaTest.class.getResourceAsStream(
                        fileName))).readAllBytes());
    }
}
