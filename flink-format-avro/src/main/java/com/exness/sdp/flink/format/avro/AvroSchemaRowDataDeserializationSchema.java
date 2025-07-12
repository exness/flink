package com.exness.sdp.flink.format.avro;

import java.io.IOException;
import java.io.Serial;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.exness.sdp.flink.format.avro.util.AvroSchemaToRowDataConverters;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class AvroSchemaRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    @Serial
    private static final long serialVersionUID = 1L;

    private final TypeInformation<RowData> resultTypeInfo;

    private final AvroSchemaToRowDataConverters.AvroToRowDataConverter runtimeConverter;

    private transient AvroKafkaDeserializer<GenericData.Record> deserializer;

    @SuppressWarnings("java:S1948")
    private final Map<String, Object> config;

    public AvroSchemaRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            Map<String, Object> config) {
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.runtimeConverter =
                AvroSchemaToRowDataConverters.createRowConverter(checkNotNull(rowType), false);
        this.config = config;
    }

    @Override
    public void open(InitializationContext context) {
        deserializer = new AvroKafkaDeserializer<>();
        deserializer.configure(config, false);
    }

    @Override
    @SuppressWarnings("java:S1181")
    public RowData deserialize(@Nullable byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            return convertToRowData(deserializer.deserialize(null, message));
        } catch (Throwable t) {
            throw new IOException(
                    format("Failed to deserialize AVRO '%s'.", new String(message)), t);
        }
    }

    public RowData convertToRowData(GenericData.Record message) {
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
        AvroSchemaRowDataDeserializationSchema that = (AvroSchemaRowDataDeserializationSchema) o;
        return resultTypeInfo.equals(that.resultTypeInfo)
                && config.equals(that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultTypeInfo, config);
    }
}
