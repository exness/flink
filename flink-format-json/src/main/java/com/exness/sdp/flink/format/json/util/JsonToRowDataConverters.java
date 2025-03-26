package com.exness.sdp.flink.format.json.util;


import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonParseException;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;


import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIME_FORMAT;

@SuppressWarnings("java:S1181")
public class JsonToRowDataConverters implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    public JsonToRowDataConverters(
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
    }

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of Flink Table & SQL internal
     * data structures.
     */
    @FunctionalInterface
    public interface JsonToRowDataConverter extends Serializable {
        Object convert(JsonNode jsonNode);
    }

    /** Creates a runtime converter which is null safe. */
    public JsonToRowDataConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private JsonToRowDataConverter createNotNullConverter(LogicalType type) {
        return switch (type.getTypeRoot()) {
            case NULL -> jsonNode -> null;
            case BOOLEAN -> this::convertToBoolean;
            case TINYINT -> jsonNode -> Byte.parseByte(jsonNode.asText().trim());
            case SMALLINT -> jsonNode -> Short.parseShort(jsonNode.asText().trim());
            case INTEGER, INTERVAL_YEAR_MONTH -> this::convertToInt;
            case BIGINT, INTERVAL_DAY_TIME -> this::convertToLong;
            case DATE -> this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE -> this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE -> this::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> this::convertToTimestampWithLocalZone;
            case FLOAT -> this::convertToFloat;
            case DOUBLE -> this::convertToDouble;
            case CHAR, VARCHAR -> this::convertToString;
            case BINARY, VARBINARY -> this::convertToBytes;
            case DECIMAL -> createDecimalConverter((DecimalType) type);
            case ARRAY -> createArrayConverter((ArrayType) type);
            case MAP -> {
                MapType mapType = (MapType) type;
                yield createMapConverter(
                        mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
            }
            case MULTISET -> {
                MultisetType multisetType = (MultisetType) type;
                yield createMapConverter(
                        multisetType.asSummaryString(),
                        multisetType.getElementType(),
                        new IntType());
            }
            case ROW -> createRowConverter((RowType) type);
            default -> throw new UnsupportedOperationException("Unsupported type: " + type);
        };
    }

    private boolean convertToBoolean(JsonNode jsonNode) {
        if (jsonNode.isBoolean()) {
            // avoid redundant toString and parseBoolean, for better performance
            return jsonNode.asBoolean();
        } else {
            return Boolean.parseBoolean(jsonNode.asText().trim());
        }
    }

    private int convertToInt(JsonNode jsonNode) {
        if (jsonNode.canConvertToInt()) {
            // avoid redundant toString and parseInt, for better performance
            return jsonNode.asInt();
        } else {
            return Integer.parseInt(jsonNode.asText().trim());
        }
    }

    private long convertToLong(JsonNode jsonNode) {
        if (jsonNode.canConvertToLong()) {
            // avoid redundant toString and parseLong, for better performance
            return jsonNode.asLong();
        } else {
            return Long.parseLong(jsonNode.asText().trim());
        }
    }

    private double convertToDouble(JsonNode jsonNode) {
        if (jsonNode.isDouble()) {
            // avoid redundant toString and parseDouble, for better performance
            return jsonNode.asDouble();
        } else {
            return Double.parseDouble(jsonNode.asText().trim());
        }
    }

    private float convertToFloat(JsonNode jsonNode) {
        if (jsonNode.isDouble()) {
            // avoid redundant toString and parseDouble, for better performance
            return (float) jsonNode.asDouble();
        } else {
            return Float.parseFloat(jsonNode.asText().trim());
        }
    }

    private int convertToDate(JsonNode jsonNode) {
        LocalDate date = ISO_LOCAL_DATE.parse(jsonNode.asText()).query(TemporalQueries.localDate());
        return (int) date.toEpochDay();
    }

    private int convertToTime(JsonNode jsonNode) {
        TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(jsonNode.asText());
        LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

        // get number of milliseconds of the day
        return localTime.toSecondOfDay() * 1000;
    }

    private TimestampData convertToTimestamp(JsonNode jsonNode) {
        TemporalAccessor parsedTimestamp = switch (timestampFormat) {
            case SQL -> SQL_TIMESTAMP_FORMAT.parse(jsonNode.asText());
            case ISO_8601 -> ISO8601_TIMESTAMP_FORMAT.parse(jsonNode.asText());
            default -> throw new TableException(
                    String.format(
                            "Unsupported timestamp format '%s'. Validator should have checked that.",
                            timestampFormat));
        };
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

        return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
    }

    private TimestampData convertToTimestampWithLocalZone(JsonNode jsonNode) {
        TemporalAccessor parsedTimestampWithLocalZone = switch (timestampFormat) {
            case SQL -> SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(jsonNode.asText());
            case ISO_8601 -> ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(jsonNode.asText());
            default -> throw new TableException(
                    String.format(
                            "Unsupported timestamp format '%s'. Validator should have checked that.",
                            timestampFormat));
        };
        LocalTime localTime = parsedTimestampWithLocalZone.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestampWithLocalZone.query(TemporalQueries.localDate());

        return TimestampData.fromInstant(
                LocalDateTime.of(localDate, localTime).toInstant(ZoneOffset.UTC));
    }

    private StringData convertToString(JsonNode jsonNode) {
        if (jsonNode.isContainerNode()) {
            return StringData.fromString(jsonNode.toString());
        } else {
            return StringData.fromString(jsonNode.asText());
        }
    }

    private byte[] convertToBytes(JsonNode jsonNode) {
        try {
            return jsonNode.binaryValue();
        } catch (IOException e) {
            throw new JsonParseException("Unable to deserialize byte array.", e);
        }
    }

    private JsonToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return jsonNode -> {
            BigDecimal bigDecimal;
            if (jsonNode.isBigDecimal()) {
                bigDecimal = jsonNode.decimalValue();
            } else {
                bigDecimal = new BigDecimal(jsonNode.asText());
            }
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private JsonToRowDataConverter createArrayConverter(ArrayType arrayType) {
        JsonToRowDataConverter elementConverter = createConverter(arrayType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        return jsonNode -> {
            final ArrayNode node = (ArrayNode) jsonNode;
            final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
            for (int i = 0; i < node.size(); i++) {
                final JsonNode innerNode = node.get(i);
                array[i] = elementConverter.convert(innerNode);
            }
            return new GenericArrayData(array);
        };
    }

    private JsonToRowDataConverter createMapConverter(
            String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final JsonToRowDataConverter keyConverter = createConverter(keyType);
        final JsonToRowDataConverter valueConverter = createConverter(valueType);

        return jsonNode -> {
            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            Map<Object, Object> result = new HashMap<>();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                Object key = keyConverter.convert(TextNode.valueOf(entry.getKey()));
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    public JsonToRowDataConverter createRowConverter(RowType rowType) {
        final JsonToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(this::createConverter)
                        .toArray(JsonToRowDataConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return jsonNode -> {
            ObjectNode node = (ObjectNode) jsonNode;
            int arity = fieldNames.length;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; i++) {
                String fieldName = fieldNames[i];
                JsonNode field = node.get(fieldName);
                try {
                    Object convertedField = convertField(fieldConverters[i], fieldName, field);
                    row.setField(i, convertedField);
                } catch (Throwable t) {
                    throw new JsonParseException(
                            String.format("Fail to deserialize at field: %s.", fieldName), t);
                }
            }
            return row;
        };
    }

    private Object convertField(
            JsonToRowDataConverter fieldConverter, String fieldName, JsonNode field) {
        if (field == null) {
            if (failOnMissingField) {
                throw new JsonParseException("Could not find field with name '" + fieldName + "'.");
            } else {
                return null;
            }
        } else {
            return fieldConverter.convert(field);
        }
    }

    private JsonToRowDataConverter wrapIntoNullableConverter(JsonToRowDataConverter converter) {
        return jsonNode -> {
            if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
                return null;
            }
            try {
                return converter.convert(jsonNode);
            } catch (Throwable t) {
                if (!ignoreParseErrors) {
                    throw t;
                }
                return null;
            }
        };
    }
}
