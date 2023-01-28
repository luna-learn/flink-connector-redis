package org.apache.flink.connector.redis.formatter;

import org.apache.flink.table.data.TimestampData;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;


/**
 * Redis formatter
 * <p/>
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/types/#data-type-extraction
 *
 * @author LiuYang
 * @date 2023/1/11 9:25
 *
 */
public class RedisFormatterUtils implements Serializable {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private static final DateTimeFormatter DATETIME_FORMATTER;

    private static final DateTimeFormatter DATETIME_LOCAL_TIMEZONE_FORMATTER;

    private static final DateTimeFormatter TIME_FORMATTER;

    static {
        // RFC3339_TIME_FORMAT = (new DateTimeFormatterBuilder()).appendPattern("HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).appendPattern("'Z'").toFormatter();
        // RFC3339_TIMESTAMP_FORMAT = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T').append(RFC3339_TIME_FORMAT).toFormatter();
        // ISO8601_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        // ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T').append(DateTimeFormatter.ISO_LOCAL_TIME).appendPattern("'Z'").toFormatter();
        TIME_FORMATTER = (new DateTimeFormatterBuilder()).appendPattern("HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
        DATETIME_FORMATTER = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(TIME_FORMATTER).toFormatter();
        DATETIME_LOCAL_TIMEZONE_FORMATTER = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(TIME_FORMATTER).appendPattern("'Z'").toFormatter();
    }

    private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};


    public static boolean isEmpty(String s) {
        return s == null || s.length() == 0;
    }

    public static boolean isNull(String s) {
        return s == null || s.length() == 0 || "null".equals(s.trim());
    }

    public static int hexAscii(char c) {
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

    public static char[] byteToHex(byte b) {
        return new char[] {
                HEX_DIGITS[(b & 0xf0) >> 4],
                HEX_DIGITS[b & 0xf]
        };
    }

    public static String bytesToHex(byte[] bytes) {
        int length = bytes.length;
        StringBuilder sb = new StringBuilder(2 * length);
        for (byte b : bytes) {
            sb.append(byteToHex(b));
        }
        return sb.toString();
    }

    public static byte hexToByte(char... hex) {
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

    public static byte[] hexToBytes(String hex) {
        if (isEmpty(hex)) {
            return null;
        }
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

    public static Boolean parseBoolean(String value) {
        if (isNull(value)) {
            return null;
        }
        return Boolean.parseBoolean(value);
    }

    public static String formatBoolean(Boolean value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    public static Byte parseByte(String value) {
        if (isNull(value)) {
            return null;
        }
        return Byte.parseByte(value);
    }

    public static String formatByte(Byte value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    public static Double parseDouble(String value) {
        if (isNull(value)) {
            return null;
        }
        return Double.parseDouble(value);
    }

    public static String formatDouble(Double value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    public static Float parseFloat(String value) {
        if (isNull(value)) {
            return null;
        }
        return Float.parseFloat(value);
    }

    public static String formatFloat(Float value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    public static Integer parseInt(String value) {
        if (isNull(value)) {
            return null;
        }
        return Integer.parseInt(value);
    }

    public static String formatInt(Integer value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    public static Long parseLong(String value) {
        if (isNull(value)) {
            return null;
        }
        return Long.parseLong(value);
    }

    public static String formatLong(Long value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    public static Short parseShort(String value) {
        if (isNull(value)) {
            return null;
        }
        return Short.parseShort(value);
    }

    public static String formatShort(Short value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    public static LocalDate parseDate(String value) {
        if (isNull(value)) {
            return null;
        }
        return LocalDate.parse(value, DATE_FORMATTER);
    }

    public static String formatDate(LocalDate value) {
        if (value == null) {
            return null;
        }
        return value.format(DATE_FORMATTER);
    }

    public static LocalTime parseTime(String value) {
        if (isNull(value)) {
            return null;
        }
        return LocalTime.parse(value, TIME_FORMATTER);
    }

    public static String formatTime(LocalTime value) {
        if (value == null) {
            return null;
        }
        return value.format(TIME_FORMATTER);
    }

    public static TimestampData parseDateTime(String value) {
        if (isNull(value)) {
            return null;
        }
        if (value.contains("T")) {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.parse(value));
        } else {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.parse(value, DATETIME_FORMATTER));
        }
    }

    public static String formatDateTime(LocalDateTime value) {
        if (value == null) {
            return null;
        }
        return value.format(DATETIME_FORMATTER);
    }

    public static TimestampData parseDateTimeWithLocalTimeZone(String value) {
        if (isNull(value)) {
            return null;
        }
        if (value.contains("T")) {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        } else {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.parse(value, DATETIME_LOCAL_TIMEZONE_FORMATTER));
        }
    }

    public static String formatDateTimeWithLocalTimeZone(LocalDateTime value) {
        if (value == null) {
            return null;
        }
        return value.format(DATETIME_LOCAL_TIMEZONE_FORMATTER);
    }
}
