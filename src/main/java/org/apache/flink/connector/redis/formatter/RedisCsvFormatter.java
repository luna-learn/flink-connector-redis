package org.apache.flink.connector.redis.formatter;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * RedisCsvFormatter
 *
 * @author LiuYang
 * @version 1.0
 * @Date 2023/4/12 9:26
 */
public class RedisCsvFormatter extends RedisBaseFormatter {

    private final CsvMapper mapper = new CsvMapper();
    private final CsvSchema csvSchema;
    private final ObjectReader reader;
    private final ObjectWriter writer;

    public RedisCsvFormatter(LogicalType logicalType, char columnSeparator, char quoteChar, char escapeChar, String lineSeparator) {
        super(logicalType);
        switch(logicalType.getTypeRoot()) {
            case ROW:
                csvSchema = convert((RowType) logicalType);
                break;
            default:
                csvSchema = covert("c0", logicalType);
        }
        csvSchema.rebuild()
                .setColumnSeparator(columnSeparator)
                .setLineSeparator(lineSeparator)
                .setEscapeChar(escapeChar)
                .setQuoteChar(quoteChar)
                .setNullValue("null")
                .build();
        reader = mapper.readerFor(JsonNode.class).with(csvSchema);
        writer = mapper.writerFor(JsonNode.class).with(csvSchema);
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
                jsonNode = reader.readTree(value);
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
        throw new UnsupportedOperationException("Csv format doesn't support map or multiset.");
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
                jsonNode = reader.readTree(value);
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
                        String.format("Redis csv format fail to deserialize row result: %s.", value), e);
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
            ArrayNode node = mapper.createArrayNode();
            int numElements = array.size();
            for (int i = 0; i < numElements; i++) {
                Object element = elementGetter.getElementOrNull(array, i);
                node.add(elementSerializer.serialize(element));
            }
            try {
                return writer.writeValueAsString(node);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis csv format fail to serialize array result: %s.", node), e);
            }
        };
    }

    public Serializer<Object, String> createMapSerializer(String typeSummary, LogicalType keyType, LogicalType valueType) {
        throw new UnsupportedOperationException("Csv format doesn't support map or multiset.");
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
            ObjectNode node = mapper.createObjectNode();
            for (int i = 0; i < fieldCount; i++) {
                String fieldName = fieldNames[i];
                try {
                    Object fieldValue = fieldGetters[i].getFieldOrNull(row);
                    if (fieldValue == null) {
                        node.putNull(fieldName);
                    } else {
                        node.put(fieldName, serializers[i].serialize(fieldValue));
                    }
                } catch (Throwable t) {
                    throw new RuntimeException(
                            String.format("Redis csv format fail to serialize row result at field: %s.", fieldName), t);
                }
            }
            try {
                return writer.writeValueAsString(node);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Redis csv format fail to serialize row result: %s.", node), e);
            }
        };
    }

    //==========================================================

    private static final HashSet<TypeInformation<?>> NUMBER_TYPES =
            new HashSet<>(
                    Arrays.asList(
                            Types.BYTE,
                            Types.SHORT,
                            Types.INT,
                            Types.LONG,
                            Types.DOUBLE,
                            Types.FLOAT,
                            Types.BIG_DEC,
                            Types.BIG_INT));

    private static final HashSet<LogicalTypeRoot> NUMBER_TYPE_ROOTS =
            new HashSet<>(
                    Arrays.asList(
                            LogicalTypeRoot.TINYINT,
                            LogicalTypeRoot.SMALLINT,
                            LogicalTypeRoot.INTEGER,
                            LogicalTypeRoot.BIGINT,
                            LogicalTypeRoot.DOUBLE,
                            LogicalTypeRoot.FLOAT,
                            LogicalTypeRoot.DECIMAL));

    private static final HashSet<TypeInformation<?>> STRING_TYPES =
            new HashSet<>(
                    Arrays.asList(
                            Types.STRING,
                            Types.SQL_DATE,
                            Types.SQL_TIME,
                            Types.SQL_TIMESTAMP,
                            Types.LOCAL_DATE,
                            Types.LOCAL_TIME,
                            Types.LOCAL_DATE_TIME,
                            Types.INSTANT));

    private static final HashSet<LogicalTypeRoot> STRING_TYPE_ROOTS =
            new HashSet<>(
                    Arrays.asList(
                            LogicalTypeRoot.CHAR,
                            LogicalTypeRoot.VARCHAR,
                            LogicalTypeRoot.BINARY,
                            LogicalTypeRoot.VARBINARY,
                            LogicalTypeRoot.DATE,
                            LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                            LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                            LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                            LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE));

    private static final HashSet<TypeInformation<?>> BOOLEAN_TYPES =
            new HashSet<>(Arrays.asList(Types.BOOLEAN, Types.VOID));

    private static final HashSet<LogicalTypeRoot> BOOLEAN_TYPE_ROOTS =
            new HashSet<>(Arrays.asList(LogicalTypeRoot.BOOLEAN, LogicalTypeRoot.NULL));


    public static CsvSchema covert(String fieldName, LogicalType logicalType) {
        CsvSchema.Builder builder = new CsvSchema.Builder();
        builder.addColumn(new CsvSchema.Column(0, fieldName, convertType(fieldName, logicalType)));
        return builder.build();
    }

    public static CsvSchema convert(RowType rowType) {
        CsvSchema.Builder builder = new CsvSchema.Builder();
        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String fieldName = fields.get(i).getName();
            LogicalType fieldType = fields.get(i).getType();
            builder.addColumn(new CsvSchema.Column(i, fieldName, convertType(fieldName, fieldType)));
        }
        return builder.build();
    }

    private static CsvSchema.ColumnType convertType(String fieldName, LogicalType type) {
        if (STRING_TYPE_ROOTS.contains(type.getTypeRoot())) {
            return CsvSchema.ColumnType.STRING;
        } else if (NUMBER_TYPE_ROOTS.contains(type.getTypeRoot())) {
            return CsvSchema.ColumnType.NUMBER;
        } else if (BOOLEAN_TYPE_ROOTS.contains(type.getTypeRoot())) {
            return CsvSchema.ColumnType.BOOLEAN;
        } else if (type.getTypeRoot() == LogicalTypeRoot.ARRAY) {
            validateNestedField(fieldName, ((ArrayType) type).getElementType());
            return CsvSchema.ColumnType.ARRAY;
        } else if (type.getTypeRoot() == LogicalTypeRoot.ROW) {
            RowType rowType = (RowType) type;
            for (LogicalType fieldType : rowType.getChildren()) {
                validateNestedField(fieldName, fieldType);
            }
            return CsvSchema.ColumnType.ARRAY;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported type '"
                            + type.asSummaryString()
                            + "' for field '"
                            + fieldName
                            + "'.");
        }
    }

    private static CsvSchema.ColumnType convertType(String fieldName, TypeInformation<?> info) {
        if (STRING_TYPES.contains(info)) {
            return CsvSchema.ColumnType.STRING;
        } else if (NUMBER_TYPES.contains(info)) {
            return CsvSchema.ColumnType.NUMBER;
        } else if (BOOLEAN_TYPES.contains(info)) {
            return CsvSchema.ColumnType.BOOLEAN;
        } else if (info instanceof ObjectArrayTypeInfo) {
            validateNestedField(fieldName, ((ObjectArrayTypeInfo) info).getComponentInfo());
            return CsvSchema.ColumnType.ARRAY;
        } else if (info instanceof BasicArrayTypeInfo) {
            validateNestedField(fieldName, ((BasicArrayTypeInfo) info).getComponentInfo());
            return CsvSchema.ColumnType.ARRAY;
        } else if (info instanceof RowTypeInfo) {
            final TypeInformation<?>[] types = ((RowTypeInfo) info).getFieldTypes();
            for (TypeInformation<?> type : types) {
                validateNestedField(fieldName, type);
            }
            return CsvSchema.ColumnType.ARRAY;
        } else if (info instanceof PrimitiveArrayTypeInfo
                && ((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
            return CsvSchema.ColumnType.STRING;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported type information '"
                            + info.toString()
                            + "' for field '"
                            + fieldName
                            + "'.");
        }
    }


    private static void validateNestedField(String fieldName, TypeInformation<?> info) {
        if (!NUMBER_TYPES.contains(info)
                && !STRING_TYPES.contains(info)
                && !BOOLEAN_TYPES.contains(info)) {
            throw new IllegalArgumentException(
                    "Only simple types are supported in the second level nesting of fields '"
                            + fieldName
                            + "' but was: "
                            + info);
        }
    }

    private static void validateNestedField(String fieldName, LogicalType type) {
        if (!NUMBER_TYPE_ROOTS.contains(type.getTypeRoot())
                && !STRING_TYPE_ROOTS.contains(type.getTypeRoot())
                && !BOOLEAN_TYPE_ROOTS.contains(type.getTypeRoot())) {
            throw new IllegalArgumentException(
                    "Only simple types are supported in the second level nesting of fields '"
                            + fieldName
                            + "' but was: "
                            + type.asSummaryString());
        }
    }

}
