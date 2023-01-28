package org.apache.flink.connector.redis.formatter;

/**
 * @author Liu Yang
 * @date 2023/1/11 12:36
 */
public enum RedisStoreStrategy {
    STRING, // Key-value
    LIST,
    SET,
    ZSET,
    HASH,
    STREAM;
}
