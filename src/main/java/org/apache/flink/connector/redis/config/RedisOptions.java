package org.apache.flink.connector.redis.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.UnsupportedRedisException;
import redis.clients.jedis.HostAndPort;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;

public class RedisOptions {
    public static final String MODE_SINGLE = "single";
    public static final String MODE_CLUSTER = "cluster";
    public static final String MODE_SENTINEL = "sentinel";

    public static final ConfigOption<String> MODE =
            ConfigOptions.key("mode")
                    .stringType()
                    .defaultValue(MODE_SINGLE)
                    .withDescription("The redis connection mode.");

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .defaultValue("localhost")
                    .withDescription("The redis host.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(6379)
                    .withDescription("The redis host.");

    public static final ConfigOption<Integer> TIMEOUT =
            ConfigOptions.key("timeout")
                    .intType()
                    .defaultValue(3000)
                    .withDescription("The redis connection timeout.");

    public static final ConfigOption<Integer> DATABASE =
            ConfigOptions.key("database")
                    .intType()
                    .defaultValue(0)
                    .withDescription("The redis database, cluster mode not support.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The redis min idle.");

    public static final ConfigOption<String> ADDITIONAL_KEY =
            ConfigOptions.key("additional-key")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("The redis sink additional Key.");

    public static final ConfigOption<Integer> MAX_TOTAL =
            ConfigOptions.key("maxTotal")
                    .intType()
                    .defaultValue(8)
                    .withDescription("The redis max total.");

    public static final ConfigOption<Integer> MAX_IDLE =
            ConfigOptions.key("maxIdle")
                    .intType()
                    .defaultValue(8)
                    .withDescription("The redis max idle.");

    public static final ConfigOption<Integer> MIN_IDLE =
            ConfigOptions.key("minIdle")
                    .intType()
                    .defaultValue(8)
                    .withDescription("The redis min idle.");

    public static final ConfigOption<String> SENTINEL_MASTER =
            ConfigOptions.key("sentinel.master")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The redis sentinel master.");

    public static final ConfigOption<String> SENTINEL_NODES =
            ConfigOptions.key("sentinel.nodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The redis sentinel nodes.");

    public static final ConfigOption<Integer> SO_TIMEOUT =
            ConfigOptions.key("so-timeout")
                    .intType()
                    .defaultValue(3000)
                    .withDescription("The redis sentinel or cluster so-timeout.");

    public static final ConfigOption<String> CLUSTER_NODES =
            ConfigOptions.key("cluster.nodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The redis cluster nodes.");

    public static final ConfigOption<Integer> CLUSTER_MAX_REDIRECTIONS =
            ConfigOptions.key("cluster.redirection.max")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The redis cluster max redirections.");

    public static final ConfigOption<Integer> LOOKUP_CACHE_MAX_SIZE=
            ConfigOptions.key("lookup.cache.max-size")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("The redis lookup max cache size.");

    public static final ConfigOption<Integer> LOOKUP_CACHE_EXPIRE_MS =
            ConfigOptions.key("lookup.cache.expire-ms")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("The redis lookup max cache expire ms.");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRY_TIMES =
            ConfigOptions.key("lookup.retry.max-times")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The redis lookup max cache expire ms.");

    public static final ConfigOption<Integer> KEY_TTL =
            ConfigOptions.key("sink.key-ttl")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("The redis sink key ttl.");

    public static final ConfigOption<Boolean> SCAN_IS_BOUNDED =
            ConfigOptions.key("scan.is-bounded")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("The redis source is bounded (default true).");

    public static HostAndPort createHostAndPort(String hostname) {
        if (Objects.isNull(hostname)) {
            return null;
        }
        String[] arr = hostname.split(":");
        if (arr.length == 1) {
            return new HostAndPort(arr[0], 6379);
        } else {
            return new HostAndPort(arr[0], Integer.parseInt(arr[1]));
        }
    }
    public static InetSocketAddress createInetSocketAddress(String hostname) {
        if (Objects.isNull(hostname)) {
            return null;
        }
        String[] arr = hostname.split(":");
        if (arr.length == 1) {
            return new InetSocketAddress(arr[0], 6379);
        } else {
            return new InetSocketAddress(arr[0], Integer.parseInt(arr[1]));
        }
    }

    public static RedisConnectorOptions getConnectorOptions(Map<String, String> properties) {
        final String mode = properties.get(MODE.key());
        final RedisConnectorOptions.Builder builder;
        switch (mode) {
            case MODE_SINGLE:
                builder = RedisConnectorOptions.withSingle()
                        .setHost(properties.get(HOST.key()))
                        .setPort(Integer.parseInt(properties.get(PORT.key())))
                        .setDatabase(Integer.parseInt(properties.get(DATABASE.key())))
                        .setPassword(properties.get(PASSWORD.key()))
                        .setTimeout(Integer.parseInt(properties.get(TIMEOUT.key())));
                break;
            case MODE_SENTINEL:
                builder = RedisConnectorOptions.withSentinel()
                        .setSentinelMaster(properties.get(SENTINEL_MASTER.key()))
                        .setSentinelNodes(properties.get(SENTINEL_NODES.key()))
                        .setSoTimeout(Integer.parseInt(properties.get(SO_TIMEOUT.key())))
                        .setDatabase(Integer.parseInt(properties.get(DATABASE.key())));

                break;
            case MODE_CLUSTER:
                builder =  RedisConnectorOptions.withCluster()
                        .setClusterNodes(properties.get(CLUSTER_NODES.key()))
                        .setMaxRedirection(Integer.parseInt(properties.get(CLUSTER_MAX_REDIRECTIONS.key())));
                break;
            default:
                throw new UnsupportedRedisException("Unsupported redis mode " + mode);
        }
        return builder.setPassword(properties.get(PASSWORD.key()))
                .setTimeout(Integer.parseInt(properties.get(TIMEOUT.key())))
                .setMaxTotal(Integer.parseInt(properties.get(MAX_TOTAL.key())))
                .build();
    }

    public static RedisConnectorOptions getConnectorOptions(ReadableConfig config) {
        final String mode = config.get(MODE);
        final RedisConnectorOptions.Builder builder;
        switch (config.get(MODE)) {
            case MODE_SINGLE:
                builder = RedisConnectorOptions.withSingle()
                        .setHost(config.get(HOST))
                        .setPort(config.get(PORT))
                        .setDatabase(config.get(DATABASE))
                        .setPassword(config.get(PASSWORD))
                        .setTimeout(config.get(TIMEOUT));
                break;
            case MODE_SENTINEL:
                builder = RedisConnectorOptions.withSentinel()
                        .setSentinelMaster(config.get(SENTINEL_MASTER))
                        .setSentinelNodes(config.get(SENTINEL_NODES))
                        .setSoTimeout(config.get(SO_TIMEOUT))
                        .setDatabase(config.get(DATABASE));

                break;
            case MODE_CLUSTER:
                builder =  RedisConnectorOptions.withCluster()
                        .setClusterNodes(config.get(CLUSTER_NODES))
                        .setSoTimeout(config.get(SO_TIMEOUT))
                        .setMaxRedirection(config.get(CLUSTER_MAX_REDIRECTIONS));
                break;
            default:
                throw new UnsupportedRedisException("Unsupported redis mode " + mode);
        }
        return builder.setPassword(config.get(PASSWORD))
                .setTimeout(config.get(TIMEOUT))
                .setMaxTotal(config.get(MAX_TOTAL))
                .build();
    }

    public static RedisSourceOptions getSourceOptions(Map<String, String> properties) {
        return new RedisSourceOptions(Integer.parseInt(properties.get(LOOKUP_CACHE_MAX_SIZE.key())),
                Integer.parseInt(properties.get(LOOKUP_CACHE_EXPIRE_MS.key())),
                Integer.parseInt(properties.get(LOOKUP_MAX_RETRY_TIMES.key())),
                Boolean.parseBoolean(properties.get(SCAN_IS_BOUNDED.key())));
    }

    public static RedisSourceOptions getSourceOptions(ReadableConfig config) {
        return new RedisSourceOptions(config.get(LOOKUP_CACHE_MAX_SIZE),
                config.get(LOOKUP_CACHE_EXPIRE_MS),
                config.get(LOOKUP_MAX_RETRY_TIMES),
                config.get(SCAN_IS_BOUNDED));
    }

    public static RedisSinkOptions getSinkOptions(ReadableConfig config) {
        return new RedisSinkOptions();
    }
}
