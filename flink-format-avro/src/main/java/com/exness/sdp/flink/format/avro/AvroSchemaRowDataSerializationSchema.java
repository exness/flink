package com.exness.sdp.flink.format.avro;

import java.io.Serial;
import java.util.Map;
import java.util.Objects;

import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;


import com.exness.sdp.flink.common.serde.kafka.SerializerWithHeaders;

import static java.lang.String.format;

public class AvroSchemaRowDataSerializationSchema implements SerializationSchema<RowData>, SerializerWithHeaders {
    @Serial
    private static final long serialVersionUID = 1L;

    private transient AvroKafkaSerializer<GenericData.Record> serializer;
    private final RowDataToAvroConverters.RowDataToAvroConverter runtimeConverter;
    private final RowType rowType;
    @SuppressWarnings("java:S1948")
    private final Map<String, Object> config;
    private final String schemaName;
    private final String schemaNamespace;
    private final Schema schema;

    public AvroSchemaRowDataSerializationSchema(
        RowType rowType,
        String schemaName,
        String schemaNamespace,
        Map<String, Object> config) {
        this.rowType = rowType;
        this.config = config;
        this.schemaName = schemaName;
        this.runtimeConverter =
            RowDataToAvroConverters.createConverter(rowType, false);
        this.schema = getSchema(rowType, schemaName, schemaNamespace);
        this.schemaNamespace = schemaNamespace;
    }

    private Schema getSchema(RowType rowType, String schemaName, String schemaNamespace) {
        Schema rowSchema = AvroSchemaConverter.convertToSchema(rowType, false);
        if (rowSchema.getType() == Schema.Type.UNION) {
            // we skip "null" value in union
            rowSchema = rowSchema.getTypes().get(1);
        }
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(schemaName).namespace(schemaNamespace);
        SchemaBuilder.FieldAssembler<Schema> fieldsBuilder = recordBuilder.fields();

        for (Schema.Field field : rowSchema.getFields()) {
            fieldsBuilder.name(field.name())
                    .type(field.schema())
                    .noDefault();
        }
        return fieldsBuilder.endRecord();
    }

    @Override
    public void open(InitializationContext context) {
        this.serializer = new AvroKafkaSerializer<>();
        this.serializer.configure(config, false);
    }

    public byte[] serialize(RowData row, Headers headers) {
        try {
            return switch (row.getRowKind()) {
                case INSERT, UPDATE_AFTER -> {
                    GenericData.Record data = (GenericData.Record) runtimeConverter.convert(schema, row);
                    yield serializer.serialize(null, headers, data);
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
        AvroSchemaRowDataSerializationSchema that = (AvroSchemaRowDataSerializationSchema) o;
        return rowType.equals(that.rowType)
                && schemaName.equals(that.schemaName)
                && schema.equals(that.schema)
                && schemaNamespace.equals(that.schemaNamespace)
                && config.equals(that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            rowType,
            schemaName,
            schema,
            schemaNamespace,
            config);
    }
}
