package org.apache.flink.connector.redis.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.FactoryUtil;

/**
 * RedisOptions
 *
 * @author Liu Yang
 * @date 2023/1/11 8:51
 */
public class RedisOptions {
    public static final String MODE_SINGLE = "single";
    public static final String MODE_CLUSTER = "cluster";
    public static final String MODE_SENTINEL = "sentinel";

    public static final ConfigOption<String> MODE =
            ConfigOptions.key("mode")
                    .stringType()
                    .defaultValue(MODE_SINGLE)
                    .withDescription("The redis connection mode.");

    public static final ConfigOption<String> TYPE =
            ConfigOptions.key("type")
                    .stringType()
                    .defaultValue("STRING")
                    .withDescription("The redis store strategy type, like STRING|SET|LIST|HASH.");

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
                    .defaultValue(30000)
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
                    .defaultValue("default::additionalKey")
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

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    public static final ConfigOption<Boolean> SCAN_IS_BOUNDED =
            ConfigOptions.key("scan.is-bounded")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("The redis source is bounded (default true).");

    public static final ConfigOption<String> STEAM_GROUP_NAME =
            ConfigOptions.key("stream.group-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The redis stream group name.");

    public static final ConfigOption<String> STEAM_ENTITY_ID =
            ConfigOptions.key("stream.entity-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The redis stream start entity id.");
}
