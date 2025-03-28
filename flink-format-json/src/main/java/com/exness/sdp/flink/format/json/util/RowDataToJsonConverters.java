package com.exness.sdp.flink.format.json.util;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIME_FORMAT;

@SuppressWarnings("java:S1181")
public class RowDataToJsonConverters implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    /** The handling mode when serializing null keys for map data. */
    private final JsonFormatOptions.MapNullKeyMode mapNullKeyMode;

    /** The string literal when handling mode for map null key LITERAL. is */
    private final String mapNullKeyLiteral;

    /** Flag indicating whether to ignore null fields. */
    private final boolean ignoreNullFields;

    public RowDataToJsonConverters(
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean ignoreNullFields) {
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
        this.ignoreNullFields = ignoreNullFields;
    }

    /**
     * Runtime converter that converts objects of Flink Table & SQL internal data structures to
     * corresponding {@link JsonNode}s.
     */
    public interface RowDataToJsonConverter extends Serializable {
        JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value);
    }

    /** Creates a runtime converter which is null safe. */
    public RowDataToJsonConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private RowDataToJsonConverter createNotNullConverter(LogicalType type) {
        return switch (type.getTypeRoot()) {
            case NULL -> (mapper, reuse, value) -> mapper.getNodeFactory().nullNode();
            case BOOLEAN -> (mapper, reuse, value) ->
                    mapper.getNodeFactory().booleanNode((boolean) value);
            case TINYINT -> (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((byte) value);
            case SMALLINT -> (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((short) value);
            case INTEGER, INTERVAL_YEAR_MONTH ->
                    (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((int) value);
            case BIGINT, INTERVAL_DAY_TIME ->
                    (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((long) value);
            case FLOAT -> (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((float) value);
            case DOUBLE -> (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((double) value);
            case CHAR, VARCHAR ->
                // value is BinaryString
                    (mapper, reuse, value) -> mapper.getNodeFactory().textNode(value.toString());
            case BINARY, VARBINARY -> (mapper, reuse, value) -> mapper.getNodeFactory().binaryNode((byte[]) value);
            case DATE -> createDateConverter();
            case TIME_WITHOUT_TIME_ZONE -> createTimeConverter();
            case TIMESTAMP_WITHOUT_TIME_ZONE -> createTimestampConverter();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> createTimestampWithLocalZone();
            case DECIMAL -> createDecimalConverter();
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
            default -> throw new UnsupportedOperationException("Not support to parse type: " + type);
        };
    }

    private RowDataToJsonConverter createDecimalConverter() {
        return (mapper, reuse, value) -> {
            BigDecimal bd = ((DecimalData) value).toBigDecimal();
            return mapper.getNodeFactory().numberNode(bd);
        };
    }

    private RowDataToJsonConverter createDateConverter() {
        return (mapper, reuse, value) -> {
            int days = (int) value;
            LocalDate date = LocalDate.ofEpochDay(days);
            return mapper.getNodeFactory().textNode(ISO_LOCAL_DATE.format(date));
        };
    }

    private RowDataToJsonConverter createTimeConverter() {
        return (mapper, reuse, value) -> {
            int millisecond = (int) value;
            LocalTime time = LocalTime.ofSecondOfDay(millisecond / 1000L);
            return mapper.getNodeFactory().textNode(SQL_TIME_FORMAT.format(time));
        };
    }

    private RowDataToJsonConverter createTimestampConverter() {
        return switch (timestampFormat) {
            case ISO_8601 -> (mapper, reuse, value) -> {
                TimestampData timestamp = (TimestampData) value;
                return mapper.getNodeFactory()
                        .textNode(ISO8601_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
            };
            case SQL -> (mapper, reuse, value) -> {
                TimestampData timestamp = (TimestampData) value;
                return mapper.getNodeFactory()
                        .textNode(SQL_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
            };
            default -> throw new TableException(
                    "Unsupported timestamp format. Validator should have checked that.");
        };
    }

    private RowDataToJsonConverter createTimestampWithLocalZone() {
        return switch (timestampFormat) {
            case ISO_8601 -> (mapper, reuse, value) -> {
                TimestampData timestampWithLocalZone = (TimestampData) value;
                return mapper.getNodeFactory()
                        .textNode(
                                ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                                        timestampWithLocalZone
                                                .toInstant()
                                                .atOffset(ZoneOffset.UTC)));
            };
            case SQL -> (mapper, reuse, value) -> {
                TimestampData timestampWithLocalZone = (TimestampData) value;
                return mapper.getNodeFactory()
                        .textNode(
                                SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                                        timestampWithLocalZone
                                                .toInstant()
                                                .atOffset(ZoneOffset.UTC)));
            };
            default -> throw new TableException(
                    "Unsupported timestamp format. Validator should have checked that.");
        };
    }

    private RowDataToJsonConverter createArrayConverter(ArrayType type) {
        final LogicalType elementType = type.getElementType();
        final RowDataToJsonConverter elementConverter = createConverter(elementType);
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        return (mapper, reuse, value) -> {
            ArrayNode node;

            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createArrayNode();
            } else {
                node = (ArrayNode) reuse;
                node.removeAll();
            }

            ArrayData array = (ArrayData) value;
            int numElements = array.size();
            for (int i = 0; i < numElements; i++) {
                Object element = elementGetter.getElementOrNull(array, i);
                node.add(elementConverter.convert(mapper, null, element));
            }

            return node;
        };
    }

    private RowDataToJsonConverter createMapConverter(
            String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final RowDataToJsonConverter valueConverter = createConverter(valueType);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        return (mapper, reuse, object) -> {
            ObjectNode node;
            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createObjectNode();
            } else {
                node = (ObjectNode) reuse;
                node.removeAll();
            }

            MapData map = (MapData) object;
            ArrayData keyArray = map.keyArray();
            ArrayData valueArray = map.valueArray();
            int numElements = map.size();
            for (int i = 0; i < numElements; i++) {
                String fieldName;
                if (keyArray.isNullAt(i)) {
                    // when map key is null
                    switch (mapNullKeyMode) {
                        case LITERAL:
                            fieldName = mapNullKeyLiteral;
                            break;
                        case DROP:
                            continue;
                        case FAIL:
                            throw new FlinkRuntimeException(
                                    String.format(
                                            "JSON format doesn't support to serialize map data with null keys. "
                                                    + "You can drop null key entries or encode null in literals by specifying %s option.",
                                            JsonFormatOptions.MAP_NULL_KEY_MODE.key()));
                        default:
                            throw new FlinkRuntimeException(
                                    "Unsupported map null key mode. Validator should have checked that.");
                    }
                } else {
                    fieldName = keyArray.getString(i).toString();
                }

                Object value = valueGetter.getElementOrNull(valueArray, i);
                node.set(fieldName, valueConverter.convert(mapper, node.get(fieldName), value));
            }

            return node;
        };
    }

    private RowDataToJsonConverter createRowConverter(RowType type) {
        final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        final LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowDataToJsonConverter[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(this::createConverter)
                        .toArray(RowDataToJsonConverter[]::new);
        final int fieldCount = type.getFieldCount();
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }

        return (mapper, reuse, value) -> {
            ObjectNode node;
            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createObjectNode();
            } else {
                node = (ObjectNode) reuse;
            }
            RowData row = (RowData) value;
            for (int i = 0; i < fieldCount; i++) {
                String fieldName = fieldNames[i];
                try {
                    Object field = fieldGetters[i].getFieldOrNull(row);
                    if (field != null || !ignoreNullFields) {
                        node.set(
                                fieldName,
                                fieldConverters[i].convert(mapper, node.get(fieldName), field));
                    }
                } catch (Throwable t) {
                    throw new FlinkRuntimeException(
                            String.format("Fail to serialize at field: %s.", fieldName), t);
                }
            }
            return node;
        };
    }

    private RowDataToJsonConverter wrapIntoNullableConverter(RowDataToJsonConverter converter) {
        return (mapper, reuse, object) -> {
            if (object == null) {
                return mapper.getNodeFactory().nullNode();
            }

            return converter.convert(mapper, reuse, object);
        };
    }
}

