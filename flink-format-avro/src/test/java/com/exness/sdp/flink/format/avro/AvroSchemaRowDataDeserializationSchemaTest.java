package com.exness.sdp.flink.format.avro;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.apicurio.registry.resolver.SchemaResolverConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class AvroSchemaRowDataDeserializationSchemaTest {

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

    private GenericRowData getTestRow() {
        GenericRowData expectedRowData = new GenericRowData(7);
        expectedRowData.setField(0, StringData.fromString("d1e37d77-e321-4fe6-ae16-7ac2e6a67e4e"));
        expectedRowData.setField(1, StringData.fromString("2025-02-10T09:50:11.839Z"));
        expectedRowData.setField(2, 418);
        expectedRowData.setField(3, StringData.fromString("trading.sot.instrument-fill-sot-script"));
        expectedRowData.setField(4, StringData.fromString("trading.sot.instrument.basic-attributes-snapshot.updated"));
        expectedRowData.setField(5, StringData.fromString("1.0"));
        GenericRowData dataRow = new GenericRowData(3);
        dataRow.setField(0, StringData.fromString("ADD"));
        dataRow.setField(1, StringData.fromString("2025-02-10T09:49:29.689545669Z"));
        GenericRowData instrumentRow = new GenericRowData(7);
        instrumentRow.setField(0, StringData.fromString("DXY"));
        instrumentRow.setField(1, StringData.fromString("US Dollar Index"));
        instrumentRow.setField(2, StringData.fromString("Indices"));
        instrumentRow.setField(3, StringData.fromString("USD"));
        instrumentRow.setField(4, StringData.fromString("USD"));
        instrumentRow.setField(5, 3);
        instrumentRow.setField(6, 1000L);
        dataRow.setField(2, instrumentRow);
        expectedRowData.setField(6, dataRow);
        return expectedRowData;
    }

    private DataType getDataType() {
        return
                ROW(
                        FIELD("id", STRING()),
                        FIELD("time", STRING()),
                        FIELD("seq_num", INT()),
                        FIELD("source", STRING()),
                        FIELD("type", STRING()),
                        FIELD("specversion", STRING()),
                        FIELD("data", ROW(
                                FIELD("action_type", STRING()),
                                FIELD("action_time", STRING()),
                                FIELD("instrument", ROW(
                                        FIELD("code", STRING()),
                                        FIELD("description", STRING()),
                                        FIELD("category", STRING()),
                                        FIELD("base_currency", STRING()),
                                        FIELD("quote_currency", STRING()),
                                        FIELD("price_precision", INT()),
                                        FIELD("contract_size", BIGINT())

                                ))
                        ))
                );
    }

    @Test
    void testApicurioDeserialization() throws IOException, InterruptedException {
        DataType dataType = getDataType();
        RowType schema = (RowType) dataType.getLogicalType();
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(schema);
        Map<String, Object> config = Map.of(SchemaResolverConfig.REGISTRY_URL,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8080),
                SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, "schema-value",
                SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE);

        AvroSchemaRowDataDeserializationSchema serde = new AvroSchemaRowDataDeserializationSchema(
                schema,
                resultTypeInfo,
                config);
        serde.open(null);

        String schemaString = new String(readResource("/schema/avro.avsc"));
        int actualSchemaId = registerArtifact("schema-value", schemaString);

        byte[] schemaId = ByteBuffer.allocate(9).putInt(5, actualSchemaId).array();
        byte[] value = readResource("/data/msg.bin");
        byte[] bytes = new byte[schemaId.length + value.length];
        System.arraycopy(schemaId, 0, bytes, 0, schemaId.length);
        System.arraycopy(value, 0, bytes, schemaId.length, value.length);
        RowData actualDeserialized = serde.deserialize(bytes);

        assertEquals(getTestRow(), actualDeserialized);
    }

    @Test
    void testConfluentDeserialization() throws IOException, InterruptedException {
        DataType dataType = getDataType();
        RowType schema = (RowType) dataType.getLogicalType();
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(schema);
        Map<String, Object> config = Map.of(SchemaResolverConfig.REGISTRY_URL,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8080),
                SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, "schema-value",
                SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE,
                ENABLE_CONFLUENT_ID_HANDLER, Boolean.TRUE,
                USE_ID, "contentId");

        AvroSchemaRowDataDeserializationSchema serde = new AvroSchemaRowDataDeserializationSchema(
                schema,
                resultTypeInfo,
                config
                );
        serde.open(null);

        String schemaString = new String(readResource("/schema/avro.avsc"));
        int actualSchemaId = registerArtifact("schema-value", schemaString);

        byte[] schemaId = ByteBuffer.allocate(5).putInt(1, actualSchemaId).array();
        byte[] value = readResource("/data/msg.bin");
        byte[] bytes = new byte[schemaId.length + value.length];
        System.arraycopy(schemaId, 0, bytes, 0, schemaId.length);
        System.arraycopy(value, 0, bytes, schemaId.length, value.length);
        RowData actualDeserialized = serde.deserialize(bytes);

        assertEquals(getTestRow(), actualDeserialized);
    }

    @Test
    void testNullDeserialization() throws IOException, InterruptedException {
        DataType dataType = ROW(
                FIELD("id", STRING()),
                FIELD("created_at", BIGINT()),
                FIELD("producer", STRING()));
        RowType schema = (RowType) dataType.getLogicalType();
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(schema);
        Map<String, Object> config = Map.of(SchemaResolverConfig.REGISTRY_URL,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8080),
                SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, "schema-value",
                SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE);

        AvroSchemaRowDataDeserializationSchema serde = new AvroSchemaRowDataDeserializationSchema(
                schema,
                resultTypeInfo,
                config);
        serde.open(null);

        String schemaString = new String(readResource("/schema/generated.avsc"));
        int actualSchemaId = registerArtifact("schema-null-value", schemaString);

        byte[] schemaId = ByteBuffer.allocate(9).putInt(5, actualSchemaId).array();
        byte[] value = readResource("/data/msg_with_null.bin");
        byte[] bytes = new byte[schemaId.length + value.length];
        System.arraycopy(schemaId, 0, bytes, 0, schemaId.length);
        System.arraycopy(value, 0, bytes, schemaId.length, value.length);
        GenericRowData expectedRowData = new GenericRowData(3);
        expectedRowData.setField(0, StringData.fromString("1"));
        expectedRowData.setField(1, null);
        expectedRowData.setField(2, StringData.fromString("producer"));
        RowData actualDeserialized = serde.deserialize(bytes);
        assertEquals(expectedRowData, actualDeserialized);
    }

    private static int registerArtifact(String subject, String schema) throws IOException, InterruptedException {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("schemaType", "AVRO");
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

    public static byte[] readResource(String fileName) throws IOException {
        return new BufferedInputStream(
                Objects.requireNonNull(AvroSchemaRowDataDeserializationSchemaTest.class.getResourceAsStream(
                        fileName))).readAllBytes();
    }
}
