package org.apache.flink.connector.redis.mapper;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.formatter.RedisStoreStrategy;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.logical.LogicalType;
import redis.clients.jedis.params.ScanParams;

import java.io.IOException;
import java.io.Serializable;
import java.util.function.Consumer;

/**
 * @author Liu Yang
 * @date 2023/1/11 9:23
 */
public interface RedisMapper<T> extends Serializable, AutoCloseable {

    String getAdditionalKey();

    Integer getKeyTtl();

    default String getFormat() {
        return "json";
    }

    ObjectIdentifier getIdentifier();

    String getPrimaryKey();

    LogicalType getResultType();

    String[] getResultFieldNames();

    LogicalType[] getResultFieldTypes();

    /**
     * 扫描 Redis
     * <p>
     * 扫描操作会扫描database下所有相关的键并进行读取操作。当扫描完所有Key时，自动结束。
     * </p>
     * @param container Redis容器
     * @param params    扫描参数
     * @param consumer  数据消费者，用于消费读取到的数据
     * @throws IOException 出现错误时抛出该异常
     */
    default void scan(RedisContainer container, ScanParams params, Consumer<T> consumer) throws IOException {
        // do nothing here
    }

    /**
     * 流式数据
     * <p>
     * 对指定的键进行流式读取操作。流式操作会一持续运行，需要手动结束。
     * </p>
     * @param container 指定 Redis 容器
     * @param consumer  数据消费者，用于消费读取到的数据
     * @throws IOException 出现错误时抛出该异常
     */
    default void stream(RedisContainer container, Consumer<T> consumer) throws IOException {
        // do nothing here
    }

    /**
     * 查询 Redis
     *
     * @param container Redis容器
     * @param key       指定键，如果已指定了additionalKey，则会组合为"$additionalKey:$key"
     * @return 返回结果实例
     * @throws IOException 出现错误时抛出该异常
     */
    T query(RedisContainer container, String key) throws IOException;

    /**
     * 更新 Redis
     * <p>
     * 如果指定了KeyTtl，则会对键设置过期时间。
     * </p>
     * @param container Redis容器
     * @param key       指定键，如果已指定了additionalKey，则会组合为"$additionalKey:$key"
     * @param value     持写入的对象，需要包含主键值，如果已指定了additionalKey，则会组合为"$additionalKey:$primaryKey"
     * @throws IOException 出现错误时抛出该异常
     */
    void upsert(RedisContainer container, String key, T value) throws IOException;

    RedisStoreStrategy getRedisStoreStrategy();

    default DeserializationSchema<T> getDeserializationSchema() {
        return null;
    }

    default SerializationSchema<T> getSerializationSchema() {
        return null;
    }


}
