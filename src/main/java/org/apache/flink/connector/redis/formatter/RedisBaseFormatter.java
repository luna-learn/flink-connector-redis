package org.apache.flink.connector.redis.formatter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * RedisBaseFormatter
 *
 * @author Liu Yang
 * @date 2023/1/16 9:23
 */
public abstract class RedisBaseFormatter implements RedisFormatter<Object, String> {

    protected final String quoteChar;
    protected final Deserializer<String, Object> runTimeDeserializer;
    protected final Serializer<Object, String>  runTimeSerializer;

    public RedisBaseFormatter(LogicalType logicalType) {
        runTimeDeserializer = createDeserializer(logicalType);
        runTimeSerializer = createSerializer(logicalType);
        quoteChar = null;
    }

    @Override
    public Object deserialize(String value) throws IOException {
        return runTimeDeserializer.deserialize(value);
    }

    @Override
    public String serialize(Object value) throws IOException {
        return runTimeSerializer.serialize(value);
    }

    private String quote(String s) {
        if (quoteChar == null) {
            return s;
        } else {
            return quoteChar + s + quoteChar;
        }
    }

    public Deserializer<String, Object> createDeserializer(LogicalType logicalType) {
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

    public abstract Deserializer<String, Object> createArrayDeserializer(ArrayType type);

    public abstract Deserializer<String, Object> createMapDeserializer(String typeSummary, LogicalType keyType, LogicalType valueType);

    public abstract Deserializer<String, Object> createRowDeserializer(RowType type);

    public Serializer<Object, String> createSerializer(LogicalType logicalType) {
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
                return value -> quote(value.toString());
            case BINARY:
            case VARBINARY:
                return value -> quote(RedisFormatterUtils.bytesToHex((byte[]) value));
            case DATE:
                return value -> quote(RedisFormatterUtils.formatDate((LocalDate) value));
            case TIME_WITHOUT_TIME_ZONE:
                return value -> quote(RedisFormatterUtils.formatTime((LocalTime) value));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return value -> {
                    LocalDateTime dateTime = ((TimestampData) value).toLocalDateTime();
                    return quote(RedisFormatterUtils.formatDateTime(dateTime));
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return value -> {
                    LocalDateTime dateTime = ((TimestampData) value).toLocalDateTime();
                    return quote(RedisFormatterUtils.formatDateTimeWithLocalTimeZone(dateTime));
                };
            case DECIMAL:
                return value -> {
                    BigDecimal decimal = ((DecimalData) value).toBigDecimal();
                    return quote(String.valueOf(decimal));
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

    public abstract Serializer<Object, String> createArraySerializer(ArrayType type);

    public abstract Serializer<Object, String> createMapSerializer(String typeSummary, LogicalType keyType, LogicalType valueType);

    public abstract Serializer<Object, String> createRowSerializer(RowType type);

}
