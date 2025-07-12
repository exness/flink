package com.exness.sdp.flink.format.json.schema.serde;

import java.time.Duration;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.apicurio.registry.resolver.SchemaResolverConfig;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static io.apicurio.registry.serde.SerdeConfig.ENABLE_HEADERS;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@Testcontainers
class FlinkJsonSchemaKafkaSerializerTest<T> extends JsonSchemaKafkaSerializer<T> implements Serializer<T> {
    @Container
    private static final GenericContainer<?> schemaRegistry = new GenericContainer(
            DockerImageName.parse("quay.io/apicurio/apicurio-registry-mem:2.5.8.Final"))
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/apis/ccompat/v7"))
            .withStartupTimeout(Duration.ofMinutes(1));

    @Test
    void schemaParserTest() {

        DataType dataType =
                ROW(
                        FIELD("id", STRING())
                );
        FlinkJsonSchemaKafkaSerializer<JsonNode> flinkJsonSchemaKafkaSerializer
                = new FlinkJsonSchemaKafkaSerializer<>((RowType) dataType.getLogicalType(), "schema");
        assertInstanceOf(FlinkJsonSchemaParser.class, flinkJsonSchemaKafkaSerializer.schemaParser());
    }

    @Test
    void testHeaders() throws JsonProcessingException {
        DataType dataType =
                ROW(
                        FIELD("id", STRING()),
                        FIELD("created_at", BIGINT()),
                        FIELD("producer", STRING())
                );
        Map<String, Object> config = Map.of(SchemaResolverConfig.REGISTRY_URL,
                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8080),
                SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, "schema-confluent-value",
                SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE,
                ENABLE_HEADERS, Boolean.TRUE);
        FlinkJsonSchemaKafkaSerializer<JsonNode> flinkJsonSchemaKafkaSerializer
                = new FlinkJsonSchemaKafkaSerializer<>((RowType) dataType.getLogicalType(), "schema");

        flinkJsonSchemaKafkaSerializer.configure(config, false);

        RecordHeaders headers = new RecordHeaders();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree("""
                {"id": "1", "created_at": 1626307200000, "producer": "producer"}
                """);
        flinkJsonSchemaKafkaSerializer.serialize(null, headers, node);
        RecordHeaders expectedHeaders = new RecordHeaders();
        expectedHeaders.add("apicurio.value.globalId", new byte[] {0, 0, 0, 0, 0, 0, 0, 1});
        expectedHeaders.add("apicurio.value.msgType", "com.fasterxml.jackson.databind.node.ObjectNode".getBytes());
        assertArrayEquals(expectedHeaders.toArray(), headers.toArray());
    }

}