package org.apache.flink.connector.redis.mapper;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Redis formatter
 * <p/>
 * https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/types/#data-type-extraction
 *
 * @author liuyang
 * @date 2022/3/1
 *
 */
public class RedisFormatter implements Serializable {
    private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private int hexAscii(char c) {
        if (c>=48 && c<=57) { // 0-9
            return c - 47;
        } else if (c>=65 && c<=70) { // A-F
            return c - 64;
        } else if (c>=97 && c<=102) { // a-f
            return c - 96;
        } else {
            return -1;
        }
    }

    private char[] byteToHex(byte b) {
        return new char[] {
                HEX_DIGITS[(b & 0xf0) >> 4],
                HEX_DIGITS[b & 0xf]
        };
    }

    private String bytesToHex(byte[] bytes) {
        int length = bytes.length;
        StringBuilder sb = new StringBuilder(2 * length);
        for (byte b : bytes) {
            sb.append(byteToHex(b));
        }
        return sb.toString();
    }

    private byte hexToByte(char... hex) {
        if (hex.length != 2) {
            throw new IllegalArgumentException("Can not parse bytes from hex string" +
                    "(" + new String(hex) + "), " +
                    "require 2 characters but given " + hex.length + " character.");
        }
        int a = hexAscii(hex[0]);
        int b = hexAscii(hex[1]);
        if (a == -1 || b == -1) {
            throw new IllegalArgumentException("Can not parse bytes from hex string" +
                    "(" + new String(hex) + "), require character range [0-9a-fA-F].");
        }
        return (byte) ((a * 255) & 0xFF + (b & 0xF));
    }

    private byte[] hexToBytes(String hex) {
        int length = hex.length() / 2; // 每两个字符描述一个字节
        byte[] ret = new byte[length];
        for (int i=0; i < length; i++) {
            try {
                ret[i] = hexToByte(hex.charAt(i * 2), hex.charAt(i * 2 + 1));
            } catch (Exception e) {
                throw new IllegalArgumentException("Can not parse bytes from hex string" +
                        "(" + hex + ") at post(" + i + ").", e);
            }
        }
        return ret;
    }

    private String encodeArrayData(ArrayData data, ArrayType arrayType) {
        final LogicalType elementType = arrayType.getElementType();
        return IntStream.range(0, data.size())
                .mapToObj(e -> encode(data, e, elementType))
                .map(String::valueOf).collect(Collectors.joining(","));
    }

    private ArrayData decodeArrayData(String data, ArrayType arrayType) {
        LogicalType elementType = arrayType.getElementType();
        return new GenericArrayData(Arrays.stream(data.split(","))
                        .map(e -> decode(e, elementType))
                        .toArray(Object[]::new));
    }

    private String encode(ArrayData data, int index, LogicalType logicalType) {
        if (data.isNullAt(index)) return null;
        switch(logicalType.getTypeRoot()) {
            case ARRAY:
                return encodeArrayData(data.getArray(index), (ArrayType) logicalType);
            case BIGINT:
                return String.valueOf(data.getLong(index));
            case BINARY:
            case VARBINARY:
                return bytesToHex(data.getBinary(index));
            case BOOLEAN:
                return String.valueOf(data.getBoolean(index));
            case CHAR:
            case VARCHAR:
                return String.valueOf(data.getString(index));
            case DECIMAL:
                DecimalType decimalType = (DecimalType)   logicalType;
                return String.valueOf(data.getDecimal(index,
                        decimalType.getPrecision(),
                        decimalType.getScale()));
            case DATE:
                int days = data.getInt(index);
                return String.valueOf(LocalDate.ofEpochDay(days));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) logicalType;
                TimestampData timestamp = data.getTimestamp(index, timestampType.getPrecision());
                return String.valueOf(timestamp);
            case INTERVAL_DAY_TIME:
                return String.valueOf(Duration.ofMillis(data.getInt(index)));
            case TIME_WITHOUT_TIME_ZONE:
                LocalTime time = LocalTime.ofNanoOfDay((long) data.getInt(index) * 1000000);
                return String.valueOf(time);
            case INTEGER:
                return String.valueOf(data.getInt(index));
            case SMALLINT:
                return String.valueOf(data.getShort(index));
            case FLOAT:
                return String.valueOf(data.getFloat(index));
            case DOUBLE:
                return String.valueOf(data.getDouble(index));
            default:
                throw new IllegalArgumentException("Unsupported encode logic type[" + logicalType + "]");
        }
    }

    public String encode(RowData data, int index, LogicalType logicalType) {
        if (data.isNullAt(index)) return null;
        switch(logicalType.getTypeRoot()) {
            case ARRAY:
                return encodeArrayData(data.getArray(index), (ArrayType) logicalType);
            case BIGINT:
                return String.valueOf(data.getLong(index));
            case BINARY:
            case VARBINARY:
                return bytesToHex(data.getBinary(index));
            case BOOLEAN:
                return String.valueOf(data.getBoolean(index));
            case CHAR:
            case VARCHAR:
                return String.valueOf(data.getString(index));
            case DECIMAL:
                DecimalType decimalType = (DecimalType)   logicalType;
                return String.valueOf(data.getDecimal(index,
                        decimalType.getPrecision(),
                        decimalType.getScale()));
            case DATE:
                int days = data.getInt(index);
                return String.valueOf(LocalDate.ofEpochDay(days));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) logicalType;
                TimestampData timestamp = data.getTimestamp(index, timestampType.getPrecision());
                return String.valueOf(timestamp);
            case INTERVAL_DAY_TIME:
                return String.valueOf(Duration.ofMillis(data.getInt(index)));
            case TIME_WITHOUT_TIME_ZONE:
                LocalTime time = LocalTime.ofNanoOfDay((long) data.getInt(index) * 1000000);
                return String.valueOf(time);
            case INTEGER:
                return String.valueOf(data.getInt(index));
            case SMALLINT:
                return String.valueOf(data.getShort(index));
            case FLOAT:
                return String.valueOf(data.getFloat(index));
            case DOUBLE:
                return String.valueOf(data.getDouble(index));
            default:
                throw new IllegalArgumentException("Unsupported encode logic type[" + logicalType + "]");
        }
    }

    public Object decode(String value, LogicalType logicalType) {
        if (value == null || "null".equals(value)) {
            return null;
        }
        switch(logicalType.getTypeRoot()) {
            case ARRAY:
                return decodeArrayData(value, (ArrayType) logicalType);
            case BINARY:
            case VARBINARY:
                return hexToBytes(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case CHAR:
            case VARCHAR:
                return StringData.fromString(value);
            case DECIMAL:
                DecimalType decimalType = (DecimalType)   logicalType;
                return DecimalData.fromBigDecimal(new BigDecimal(value), decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromLocalDateTime(LocalDateTime.parse(value));
            case DATE:
                LocalDate date = LocalDate.parse(value);
                return date == null ? null : (int) date.toEpochDay();
            case INTERVAL_DAY_TIME:
                return Duration.parse(value);
            case TIME_WITHOUT_TIME_ZONE:
                LocalTime time = LocalTime.parse(value);
                return time == null ? null : (int) (time.toNanoOfDay() / 1000000);
            case INTEGER:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case SMALLINT:
                return Short.parseShort(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            default:
                throw new IllegalArgumentException("Unsupported decode logic type[" + logicalType + "]");
        }
        // org.apache.flink.streaming.runtime.io.RecordWriterOutput
    }

    public RedisFormatter() {

    }
}
