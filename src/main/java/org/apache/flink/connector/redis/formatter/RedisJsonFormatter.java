package org.apache.flink.connector.redis.formatter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * RedisJsonFormatter
 *
 * @author Liu Yang
 * @date 2023/1/16 9:23
 */
public class RedisJsonFormatter extends RedisBaseFormatter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public RedisJsonFormatter(LogicalType logicalType) {
        super(logicalType);
        objectMapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);
        objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        objectMapper.enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT);
    }

    @Override
    public Deserializer<String, Object> createArrayDeserializer(ArrayType type) {
        final LogicalType elementType = type.getElementType();
        final Deserializer<String, Object> deserializer = createDeserializer(elementType);
        return value -> {
            final JsonNode jsonNode;
            final ArrayNode arrayNode;
            final Object[] values;
            try {
                jsonNode = objectMapper.readTree(value);
                if (jsonNode.isArray()) {
                    arrayNode = (ArrayNode) jsonNode;
                    final int fieldCount = arrayNode.size();
                    values = new Object[arrayNode.size()];
                    for(int i = 0; i < fieldCount; i++) {
                        JsonNode element = arrayNode.get(i);
                        if (element.isNull()) {
                            values[i] = null;
                        } else {
                            values[i] = deserializer.deserialize(element.asText());
                        }
                    }
                    return new GenericArrayData(values);
                } else {
                    throw new RuntimeException("Unsupported non-array data " + jsonNode);
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis json format fail to deserialize map result: %s.", value), e);
            }
        };
    }

    @Override
    public Deserializer<String, Object> createMapDeserializer(String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!keyType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final Deserializer<String, Object> keyDeserializer = createDeserializer(keyType);
        final Deserializer<String, Object> valueDeserializer = createDeserializer(valueType);
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
                            data.put(keyDeserializer.deserialize(nodeKey), null);
                        } else {
                            data.put(keyDeserializer.deserialize(nodeKey),
                                    valueDeserializer.deserialize(nodeValue.textValue()));
                        }

                    }
                    return new GenericMapData(data);
                } else {
                    throw new RuntimeException("Unsupported non-object data " + jsonNode);
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis json format fail to deserialize map result: %s.", value), e);
            }
        };
    }

    @Override
    public Deserializer<String, Object> createRowDeserializer(RowType type) {
        final List<RowType.RowField> fields = type.getFields();
        final int fieldCount = fields.size();
        final String[] fieldNames = new String[fieldCount];
        final Deserializer<String, Object>[] fieldDeserializers = new Deserializer[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldNames[i] = fields.get(i).getName();
            fieldDeserializers[i] = this.createDeserializer(fields.get(i).getType());
        }
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
                                Object deserializedField = fieldDeserializers[i].deserialize(field.textValue());
                                row.setField(i, deserializedField);
                            }
                        } catch (Throwable t) {
                            throw new RuntimeException (
                                    String.format("Fail to deserialize at field: %s.", fieldName), t);
                        }
                    }
                    return row;
                } else {
                    throw new RuntimeException("Unsupported non-object data " + jsonNode);
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis json format fail to deserialize row result: %s.", value), e);
            }
        };
    }

    @Override
    public Serializer<Object, String> createArraySerializer(ArrayType type) {
        final LogicalType elementType = type.getElementType();
        final Serializer<Object, String> elementSerializer = createSerializer(elementType);
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        return value -> {
            ArrayData array = (ArrayData) value;
            Object[] data = new Object[array.size()];
            int numElements = array.size();
            for (int i = 0; i < numElements; i++) {
                Object element = elementGetter.getElementOrNull(array, i);
                data[i] = elementSerializer.serialize(element);
            }
            try {
                return objectMapper.writeValueAsString(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis json format fail to serialize array result: %s.", data), e);
            }
        };
    }

    public Serializer<Object, String> createMapSerializer(String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!keyType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "Redis json format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final Serializer<Object, String> valueSerializer = createSerializer(valueType);
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
                    data.put(fieldName, valueSerializer.serialize(fieldValue));
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

    @Override
    public Serializer<Object, String> createRowSerializer(RowType type) {
        final List<RowType.RowField> fields = type.getFields();
        final int fieldCount = fields.size();
        final String[] fieldNames = new String[fieldCount];
        final LogicalType[] fieldTypes = new LogicalType[fieldCount];
        final Serializer<Object, String>[] serializers = new Serializer[fieldCount];
        for (int i = 0; i < fields.size(); i++) {
            fieldNames[i] = fields.get(i).getName();
            fieldTypes[i] = fields.get(i).getType();
            serializers[i] = this.createSerializer(fields.get(i).getType());
        }
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldCount];
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
                        data.put(fieldName, serializers[i].serialize(fieldValue));
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
