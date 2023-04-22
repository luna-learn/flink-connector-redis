package org.apache.flink.connector.redis.formatter;

/**
 * Deserializer
 *
 * @author LiuYang
 * @version 1.0
 * @Date 2023/4/12 11:09
 */
@FunctionalInterface
public interface Serializer<T, R> {
    R serialize(T value);
}
