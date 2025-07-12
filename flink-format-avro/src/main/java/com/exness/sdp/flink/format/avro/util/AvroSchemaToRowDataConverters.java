package com.exness.sdp.flink.format.avro.util;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.extractValueTypeToAvroMap;

@SuppressWarnings({"java:S6201"})
public class AvroSchemaToRowDataConverters {
    @FunctionalInterface
    public interface AvroToRowDataConverter extends Serializable {
        Object convert(Object object);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    public static AvroSchemaToRowDataConverters.AvroToRowDataConverter createRowConverter(RowType rowType) {
        return createRowConverter(rowType, true);
    }

    public static AvroSchemaToRowDataConverters.AvroToRowDataConverter createRowConverter(
            RowType rowType, boolean legacyTimestampMapping) {
        final AvroSchemaToRowDataConverters.AvroToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(type -> createNullableConverter(type, legacyTimestampMapping))
                        .toArray(AvroSchemaToRowDataConverters.AvroToRowDataConverter[]::new);
        final int arity = rowType.getFieldCount();

        return avroObject -> {
            GenericRecord rec = (GenericRecord) avroObject;
            GenericRowData row = new GenericRowData(arity);
            List<RowType.RowField> fields = rowType.getFields();
            for (int i = 0; i < fields.size(); i++) {
                RowType.RowField field = fields.get(i);
                if (rec.hasField(field.getName())) {
                    row.setField(
                            i,
                            fieldConverters[i].convert(rec.get(field.getName())));
                } else {
                    row.setField(i, null);
                }
            }
            return row;
        };
    }

    /** Creates a runtime converter which is null safe. */
    private static AvroSchemaToRowDataConverters.AvroToRowDataConverter createNullableConverter(
            LogicalType type, boolean legacyTimestampMapping) {
        final AvroSchemaToRowDataConverters.AvroToRowDataConverter converter = createConverter(type, legacyTimestampMapping);
        return avroObject -> {
            if (avroObject == null) {
                return null;
            }
            return converter.convert(avroObject);
        };
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private static AvroSchemaToRowDataConverters.AvroToRowDataConverter createConverter(
            LogicalType type, boolean legacyTimestampMapping) {
        switch (type.getTypeRoot()) {
            case NULL:
                return avroObject -> null;
            case TINYINT:
                return avroObject -> ((Integer) avroObject).byteValue();
            case SMALLINT:
                return avroObject -> ((Integer) avroObject).shortValue();
            case BOOLEAN, INTEGER, INTERVAL_YEAR_MONTH, BIGINT, INTERVAL_DAY_TIME, FLOAT, DOUBLE:
                return avroObject -> avroObject;
            case DATE:
                return AvroSchemaToRowDataConverters::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return AvroSchemaToRowDataConverters::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return AvroSchemaToRowDataConverters::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (legacyTimestampMapping) {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                } else {
                    return AvroSchemaToRowDataConverters::convertToTimestamp;
                }
            case CHAR, VARCHAR:
                return avroObject -> StringData.fromString(avroObject.toString());
            case BINARY, VARBINARY:
                return AvroSchemaToRowDataConverters::convertToBytes;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type, legacyTimestampMapping);
            case ROW:
                return createRowConverter((RowType) type);
            case MAP, MULTISET:
                return createMapConverter(type, legacyTimestampMapping);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static AvroSchemaToRowDataConverters.AvroToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return avroObject -> {
            final byte[] bytes;
            if (avroObject instanceof GenericFixed) {
                bytes = ((GenericFixed) avroObject).bytes();
            } else if (avroObject instanceof ByteBuffer) {
                ByteBuffer byteBuffer = (ByteBuffer) avroObject;
                bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
            } else {
                bytes = (byte[]) avroObject;
            }
            return DecimalData.fromUnscaledBytes(bytes, precision, scale);
        };
    }

    private static AvroSchemaToRowDataConverters.AvroToRowDataConverter createArrayConverter(
            ArrayType arrayType, boolean legacyTimestampMapping) {
        final AvroSchemaToRowDataConverters.AvroToRowDataConverter elementConverter =
                createNullableConverter(arrayType.getElementType(), legacyTimestampMapping);
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());

        return avroObject -> {
            final List<?> list = (List<?>) avroObject;
            final int length = list.size();
            final Object[] array = (Object[]) Array.newInstance(elementClass, length);
            for (int i = 0; i < length; ++i) {
                array[i] = elementConverter.convert(list.get(i));
            }
            return new GenericArrayData(array);
        };
    }

    private static AvroSchemaToRowDataConverters.AvroToRowDataConverter createMapConverter(
            LogicalType type, boolean legacyTimestampMapping) {
        final AvroSchemaToRowDataConverters.AvroToRowDataConverter keyConverter =
                createConverter(DataTypes.STRING().getLogicalType(), legacyTimestampMapping);
        final AvroSchemaToRowDataConverters.AvroToRowDataConverter valueConverter =
                createNullableConverter(extractValueTypeToAvroMap(type), legacyTimestampMapping);

        return avroObject -> {
            final Map<?, ?> map = (Map<?, ?>) avroObject;
            Map<Object, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = keyConverter.convert(entry.getKey());
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    private static TimestampData convertToTimestamp(Object object) {
        final long millis;
        if (object instanceof Long) {
            millis = (Long) object;
        } else if (object instanceof Instant) {
            millis = ((Instant) object).toEpochMilli();
        } else if (object instanceof LocalDateTime) {
            return TimestampData.fromLocalDateTime((LocalDateTime) object);
        } else {
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                millis = jodaConverter.convertTimestamp(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for TIMESTAMP logical type. Received: " + object);
            }
        }
        return TimestampData.fromEpochMillis(millis);
    }

    private static int convertToDate(Object object) {
        if (object instanceof Integer) {
            return (Integer) object;
        } else if (object instanceof LocalDate) {
            return (int) ((LocalDate) object).toEpochDay();
        } else {
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                return (int) jodaConverter.convertDate(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for DATE logical type. Received: " + object);
            }
        }
    }

    private static int convertToTime(Object object) {
        final int millis;
        if (object instanceof Integer) {
            millis = (Integer) object;
        } else if (object instanceof LocalTime) {
            millis = ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
        } else {
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                millis = jodaConverter.convertTime(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for TIME logical type. Received: " + object);
            }
        }
        return millis;
    }

    private static byte[] convertToBytes(Object object) {
        if (object instanceof GenericFixed) {
            return ((GenericFixed) object).bytes();
        } else if (object instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) object;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            return (byte[]) object;
        }
    }
}
