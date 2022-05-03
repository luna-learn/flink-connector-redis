package org.apache.flink.connector.redis.container;

import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RedisContainer {

    void open() throws Exception;

    void close();

    /**
     * 获取配置连接配置
     * @return 返回连接配置实例对象
     */
    RedisConnectorOptions getOptions();

    /**
     * 开始一个事务
     * 可以通过调用返回的事务实例方法实现commit和rollback功能。
     * 单机或者哨兵主从模式支持，集群模式不支持
     * @return 返回事务实例
     */
    Transaction multi();

    void expire(String key, long seconds);

    void del(String key);

    void set(String key, String value);

    String get(String key);

    ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params);

    void hdel(String key, String field);

    long hlen(String key);

    default ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        return hscan(key, cursor, new ScanParams());
    }

    void hset(String key, String field, String value);

    // void hset(String key, String field, byte[] value);

    void hset(String key, String field, String value, int ttl);

    void hmset(String key, Map<String, String> values);

    String hget(String key, String field);

    // byte[] hget2(String key, String field);

    List<String> hmget(String key, String... fields);

    Set<String> keys(String pattern);

    Set<String> hkeys(String key);

    String type(String key);
}
