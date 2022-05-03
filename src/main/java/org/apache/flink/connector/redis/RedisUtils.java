package org.apache.flink.connector.redis;

import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisOptions;
import org.apache.flink.connector.redis.container.RedisContainer;

import java.util.Map;
import java.util.Set;

/**
 * @author Liu Yang
 * @date 2022/3/14 15:23
 */
public final class RedisUtils {
    private static RedisConnectorOptions connectorOptions;
    private static RedisContainer redisContainer;

    public static void withSingle(String host, int port, int timeout, String password) {
        connectorOptions = RedisConnectorOptions.withSingle()
                .setHost(host)
                .setPort(port)
                .setTimeout(timeout)
                .setPassword(password)
                .build();
    }

    public static void withSentinel(String master, Set<String> nodes, int timeout, int soTimeout, String password) {
        connectorOptions = RedisConnectorOptions.withSentinel()
                .setSentinelMaster(master)
                .setSentinelNodes(String.join(",", nodes))
                .setSoTimeout(soTimeout)
                .setTimeout(timeout)
                .setPassword(password)
                .build();
    }

    public static void withCluster(Set<String> nodes, int timeout, int soTimeout, String password) {
        connectorOptions = RedisConnectorOptions.withCluster()
                .setClusterNodes(String.join(",", nodes))
                .setTimeout(timeout)
                .setPassword(password)
                .build();
    }

    public static void withConfig(Map<String, String> conf) {
        connectorOptions = RedisOptions.getConnectorOptions(conf);
    }

    private static RedisContainer getContainer() {
        if (connectorOptions == null) {
            throw new NullPointerException("RedisUtils connectorOptions property is null.");
        }
        if (redisContainer == null) {
            redisContainer = connectorOptions.getContainer();
        }
        return redisContainer;
    }

    public static void del(String key) {
        getContainer().del(key);
    }

    public static String get(String key) {
        return getContainer().get(key);
    }

    public static void set(String key, String value) {
        getContainer().set(key, value);
    }

    public static void hdel(String key, String field) {
        getContainer().hdel(key, field);
    }

    public static String hget(String key, String field) {
        return getContainer().hget(key, field);
    }

    public static void hset(String key, String field, String value) {
        getContainer().hset(key, field, value);
    }
}
