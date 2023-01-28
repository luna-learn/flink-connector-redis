package org.apache.flink.connector.redis.formatter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * RedisBaseRowDataFormatter
 * @author Liu Yang
 * @date 2023/1/16 9:23
 */
public class RedisJsonFormatter implements RedisFormatter<Object, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Deserializer runTimeDeserializer;
    private final Serializer runTimeSerializer;

    public RedisJsonFormatter(LogicalType logicalType) {
        runTimeDeserializer = createDeserializer(logicalType);
        runTimeSerializer = createSerializer(logicalType);
        objectMapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);
        objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        objectMapper.enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT);
    }

    @Override
    public Object deserialize(String value) throws IOException {
        return runTimeDeserializer.convert(value);
    }

    @Override
    public String serialize(Object value) throws IOException {
        return runTimeSerializer.convert(value);
    }

    //===========================================================
    public interface Deserializer {
        Object convert(String value);
    }

    public interface Serializer {
        String convert(Object value);
    }

    private Deserializer createDeserializer(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case NULL:
                return (value) -> null;
            case BOOLEAN:
                return RedisFormatterUtils::parseBoolean;
            case TINYINT:
                return RedisFormatterUtils::parseByte;
            case SMALLINT:
                return RedisFormatterUtils::parseShort;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return RedisFormatterUtils::parseInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return RedisFormatterUtils::parseLong;
            case FLOAT:
                return RedisFormatterUtils::parseFloat;
            case DOUBLE:
                return RedisFormatterUtils::parseDouble;
            case CHAR:
            case VARCHAR:
                return StringData::fromString;
            case BINARY:
            case VARBINARY:
                return RedisFormatterUtils::hexToBytes;
            case DATE:
                return RedisFormatterUtils::parseDate;
            case TIME_WITHOUT_TIME_ZONE:
                return RedisFormatterUtils::parseTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return RedisFormatterUtils::parseDateTime;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return RedisFormatterUtils::parseDateTimeWithLocalTimeZone;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                final int precision = decimalType.getPrecision();
                final int scale = decimalType.getScale();
                return value -> DecimalData.fromBigDecimal(new BigDecimal(value), precision, scale);
            case ARRAY:
                return createArrayDeserializer((ArrayType) logicalType);
            case MAP:
                MapType mapType = (MapType) logicalType;
                return createMapDeserializer(mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
            case MULTISET:
                MultisetType multisetType = (MultisetType) logicalType;
                return createMapDeserializer(multisetType.asSummaryString(),
                        multisetType.getElementType(),
                        new IntType());
            case ROW:
                return createRowDeserializer((RowType) logicalType);
            case RAW:
            default:
                throw new UnsupportedOperationException("Not support to parse type: " + logicalType);
        }
    }

    private Deserializer createArrayDeserializer(ArrayType type) {
        final LogicalType elementType = type.getElementType();
        final Deserializer elementDeserializer = createDeserializer(elementType);
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(type.getElementType());
        return value -> {
            final JsonNode jsonNode;
            final Object[] data;
            try {
                jsonNode = objectMapper.readTree(value);
                if (jsonNode.isArray()) {
                    int nodeSize = jsonNode.size();
                    data = (Object[]) Array.newInstance(elementClass, nodeSize);
                    for (int i = 0; i < jsonNode.size(); i++) {
                        final JsonNode innerNode = jsonNode.get(i);
                        data[i] = elementDeserializer.convert(innerNode.textValue());
                    }
                    return new GenericArrayData(data);
                } else {
                    throw new RuntimeException("Unsupported non-array data");
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis json format fail to deserialize array result: %s.", value), e);
            }
        };
    }

    private Deserializer createMapDeserializer(String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final Deserializer keyDeserializer = createDeserializer(keyType);
        final Deserializer valueDeserializer = createDeserializer(valueType);
        return value -> {
            final JsonNode jsonNode;
            final Map<Object, Object> data;
            try {
                jsonNode = objectMapper.readTree(value);
                if (jsonNode.isObject()) {
                    Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
                    Map.Entry<String, JsonNode> entry;
                    data = new LinkedHashMap<>();
                    while(fields.hasNext()) {
                        entry = fields.next();
                        String nodeKey = entry.getKey();
                        JsonNode nodeValue = entry.getValue();
                        if (nodeKey == null) {
                            continue;
                        }
                        if (nodeValue == null) {
                            data.put(keyDeserializer.convert(nodeKey), null);
                        } else {
                            data.put(keyDeserializer.convert(nodeKey),
                                    valueDeserializer.convert(nodeValue.textValue()));
                        }

                    }
                    return new GenericMapData(data);
                } else {
                    throw new RuntimeException("Unsupported non-object data");
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis json format fail to deserialize map result: %s.", value), e);
            }
        };
    }

    private Deserializer createRowDeserializer(RowType type) {
        final Deserializer[] fieldDeserializer =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(this::createDeserializer)
                        .toArray(Deserializer[]::new);
        final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        return value -> {
            JsonNode jsonNode;
            try {
                jsonNode = objectMapper.readTree(value);
                if (jsonNode.isObject()) {
                    int arity = fieldNames.length;
                    GenericRowData row = new GenericRowData(arity);
                    for (int i = 0; i < arity; i++) {
                        String fieldName = fieldNames[i];
                        JsonNode field = jsonNode.get(fieldName);
                        try {
                            if (field == null) {
                                row.setField(i, null);
                            } else {
                                Object deserializedField = fieldDeserializer[i].convert(field.textValue());
                                row.setField(i, deserializedField);
                            }
                        } catch (Throwable t) {
                            throw new RuntimeException (
                                    String.format("Fail to deserialize at field: %s.", fieldName), t);
                        }
                    }
                    return row;
                } else {
                    throw new RuntimeException("Unsupported non-object data");
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis json format fail to deserialize row result: %s.", value), e);
            }
        };
    }

    private Serializer createSerializer(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case NULL:
                return value -> "null";
            case BOOLEAN:
                return value -> RedisFormatterUtils.formatBoolean((Boolean) value);
            case TINYINT:
                return value -> RedisFormatterUtils.formatByte((Byte) value);
            case SMALLINT:
                return value -> RedisFormatterUtils.formatShort((Short) value);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return value -> RedisFormatterUtils.formatInt((Integer) value);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return value -> RedisFormatterUtils.formatLong((Long) value);
            case FLOAT:
                return value -> RedisFormatterUtils.formatFloat((Float) value);
            case DOUBLE:
                return value -> RedisFormatterUtils.formatDouble((Double) value);
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return Object::toString;
            case BINARY:
            case VARBINARY:
                return value -> RedisFormatterUtils.bytesToHex((byte[]) value);
            case DATE:
                return value -> RedisFormatterUtils.formatDate((LocalDate) value);
            case TIME_WITHOUT_TIME_ZONE:
                return value -> RedisFormatterUtils.formatTime((LocalTime) value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return value -> {
                    LocalDateTime dateTime = ((TimestampData) value).toLocalDateTime();
                    return RedisFormatterUtils.formatDateTime(dateTime);
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return value -> {
                    LocalDateTime dateTime = ((TimestampData) value).toLocalDateTime();
                    return RedisFormatterUtils.formatDateTimeWithLocalTimeZone(dateTime);
                };
            case DECIMAL:
                return value -> {
                    BigDecimal decimal = ((DecimalData) value).toBigDecimal();
                    return String.valueOf(decimal);
                };
            case ARRAY:
                return createArraySerializer((ArrayType) logicalType);
            case MAP:
                MapType mapType = (MapType) logicalType;
                return createMapSerializer(mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
            case MULTISET:
                MultisetType multisetType = (MultisetType) logicalType;
                return createMapSerializer(multisetType.asSummaryString(),
                        multisetType.getElementType(),
                        new IntType());
            case ROW:
                return createRowSerializer((RowType) logicalType);
            case RAW:
            default:
                throw new UnsupportedOperationException("Redis json format not support to parse type: " + logicalType);
        }
    }

    private Serializer createArraySerializer(ArrayType type) {
        final LogicalType elementType = type.getElementType();
        final Serializer elementSerializer = createSerializer(elementType);
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        return value -> {
            ArrayData array = (ArrayData) value;
            Object[] data = new Object[array.size()];
            int numElements = array.size();
            for (int i = 0; i < numElements; i++) {
                Object element = elementGetter.getElementOrNull(array, i);
                data[i] = elementSerializer.convert(element);
            }
            try {
                return objectMapper.writeValueAsString(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis json format fail to serialize array result: %s.", data), e);
            }
        };
    }

    private Serializer createMapSerializer(String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "Redis json format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final Serializer valueSerializer = createSerializer(valueType);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        return value -> {
            MapData map = (MapData) value;
            ArrayData keyArray = map.keyArray();
            ArrayData valueArray = map.valueArray();
            Map<String, Object> data = new LinkedHashMap<>();
            int numElements = map.size();
            for (int i = 0; i < numElements; i++) {
                String fieldName;
                if (keyArray.isNullAt(i)) {
                    continue;
                } else {
                    fieldName = keyArray.getString(i).toString();
                }
                Object fieldValue = valueGetter.getElementOrNull(valueArray, i);
                if (fieldValue == null) {
                    data.put(fieldName, null);
                } else {
                    data.put(fieldName, valueSerializer.convert(fieldValue));
                }
            }
            try {
                return objectMapper.writeValueAsString(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis json format fail to serialize map result: %s.", map), e);
            }
        };
    }

    private Serializer createRowSerializer(RowType type) {
        final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        final LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final Serializer[] serializers =  Arrays.stream(fieldTypes)
                .map(this::createSerializer)
                .toArray(Serializer[]::new);
        final int fieldCount = type.getFieldCount();
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }
        return value -> {
            RowData row = (RowData) value;
            Map<String, String> data = new LinkedHashMap<>();
            for (int i = 0; i < fieldCount; i++) {
                String fieldName = fieldNames[i];
                try {
                    Object fieldValue = fieldGetters[i].getFieldOrNull(row);
                    if (fieldValue == null) {
                        data.put(fieldName, null);
                    } else {
                        data.put(fieldName, serializers[i].convert(fieldValue));
                    }
                } catch (Throwable t) {
                    throw new RuntimeException(
                            String.format("Redis json format fail to serialize row result at field: %s.", fieldName), t);
                }
            }
            try {
                return objectMapper.writeValueAsString(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis json format fail to serialize row result: %s.", data), e);
            }
        };
    }
}
