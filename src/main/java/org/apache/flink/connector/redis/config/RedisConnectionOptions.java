package org.apache.flink.connector.redis.config;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.RedisUnsupportedException;
import org.apache.flink.connector.redis.container.RedisClusterContainer;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.container.RedisPoolContainer;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RedisConnectionOptions
 *
 * @author Liu Yang
 * @date 2023/1/11 8:21
 */
public class RedisConnectionOptions implements Serializable {

    private final static Map<RedisConnectionOptions, RedisContainer> containerPool = new ConcurrentHashMap<>();

    // common
    private String mode;
    private String password;
    private int timeout;
    private int maxTotal;
    private int maxIdle;
    private int minIdle;
    // single
    private String host;
    private int port;
    private int database; // single && sentinel
    // sentinel
    private String sentinelMaster;
    private String sentinelNodes;
    private int soTimeout;
    // cluster
    private String clusterNodes;
    private int maxRedirection;


    private RedisConnectionOptions() {

    }

    public String getMode() {
        return mode;
    }

    public String getPassword() {
        return password;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getDatabase() {
        return database;
    }

    public String getSentinelMaster() {
        return sentinelMaster;
    }

    public String getSentinelNodes() {
        return sentinelNodes;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public String getClusterNodes() {
        return clusterNodes;
    }

    public int getMaxRedirection() {
        return maxRedirection;
    }

    public RedisContainer getContainer() {
        if (containerPool.containsKey(this)) {
            return containerPool.get(this);
        }
        RedisContainer container;
        switch(mode) {
            case RedisOptions.MODE_SINGLE:
            case RedisOptions.MODE_SENTINEL:
                container = new RedisPoolContainer(this);
                break;
            case RedisOptions.MODE_CLUSTER:
                container = new RedisClusterContainer(this);
                break;
            default:
                throw new RedisUnsupportedException("Unsupported redis mode " + mode);
        }
        containerPool.put(this, container);
        return container;
    }

    public static SingleBuilder withSingle() {
        return new SingleBuilder();
    }

    public static SentinelBuilder withSentinel() {
        return new SentinelBuilder();
    }

    public static ClusterBuilder withCluster() {
        return new ClusterBuilder();
    }

    public static abstract class Builder {
        protected String mode;
        protected String password;
        protected int database;
        protected int timeout;
        protected int maxTotal;
        protected int maxIdle;
        protected int minIdle;
        protected String host;
        protected int port;
        protected String sentinelMaster;
        protected String sentinelNodes;
        protected int soTimeout;
        protected String clusterNodes;
        protected int maxRedirection;

        private Builder() {
            this.maxTotal = 8;
            this.maxIdle = 8;
            this.minIdle = 0;
            this.timeout = 2000;
            this.database = 0;
        }

        public Builder setPassword(String password) {
            if (password == null || password.length() == 0) {
                this.password = null;
            } else {
                this.password = password;
            }
            return this;
        }

        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        public Builder setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        public Builder setMinIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public RedisConnectionOptions build() {
            RedisConnectionOptions options = new RedisConnectionOptions();
            options.mode = this.mode;
            options.password = this.password;
            options.timeout = this.timeout;
            options.maxTotal = this.maxTotal;
            options.maxIdle = this.maxIdle;
            options.minIdle = this.minIdle;
            options.host = this.host;
            options.port = this.port;
            options.database = this.database;
            options.sentinelMaster = this.sentinelMaster;
            options.sentinelNodes = this.sentinelNodes;
            options.soTimeout = this.soTimeout;
            options.clusterNodes = this.clusterNodes;
            options.maxRedirection = this.maxRedirection;
            return options;
        }

    }

    public static class SingleBuilder extends Builder {

        private SingleBuilder() {
            super();
            this.mode = RedisOptions.MODE_SINGLE;
            this.host = "localhost";
            this.port = 6397;
            this.password = null;
        }

        public SingleBuilder setHost(String host) {
            this.host = host;
            return this;
        }

        public SingleBuilder setPort(int port) {
            this.port = port;
            return this;
        }

        public SingleBuilder setDatabase(int database) {
            this.database = database;
            return this;
        }
    }

    public static class SentinelBuilder extends Builder {
        private SentinelBuilder() {
            super();
            this.mode = RedisOptions.MODE_SENTINEL;
            this.soTimeout = 2000;
        }

        public SentinelBuilder setSentinelMaster(String sentinelMaster) {
            this.sentinelMaster = sentinelMaster;
            return this;
        }

        public SentinelBuilder setSentinelNodes(String sentinelNodes) {
            this.sentinelNodes = sentinelNodes;
            return this;
        }

        public SentinelBuilder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public SentinelBuilder setSoTimeout(int soTimeout) {
            this.soTimeout = soTimeout;
            return this;
        }
    }

    public static class ClusterBuilder extends Builder {
        private ClusterBuilder() {
            super();
            this.mode = RedisOptions.MODE_CLUSTER;
        }

        public ClusterBuilder setClusterNodes(String clusterNodes) {
            this.clusterNodes = clusterNodes;
            return this;
        }

        public ClusterBuilder setMaxRedirection(int maxRedirection) {
            this.maxRedirection = maxRedirection;
            return this;
        }

        public ClusterBuilder setSoTimeout(int soTimeout) {
            this.soTimeout = soTimeout;
            return this;
        }
    }

    public static RedisConnectionOptions getConnectorOptions(Map<String, String> properties) {
        final String mode = properties.get(RedisOptions.MODE.key());
        final Builder builder;
        switch (mode) {
            case RedisOptions.MODE_SINGLE:
                builder = RedisConnectionOptions.withSingle()
                        .setHost(properties.getOrDefault(RedisOptions.HOST.key(), RedisOptions.HOST.defaultValue()))
                        .setPort(Integer.parseInt(properties.getOrDefault(RedisOptions.PORT.key(), String.valueOf(RedisOptions.PORT.defaultValue()))))
                        .setDatabase(Integer.parseInt(properties.getOrDefault(RedisOptions.DATABASE.key(), String.valueOf(RedisOptions.DATABASE.defaultValue()))))
                        .setPassword(properties.getOrDefault(RedisOptions.PASSWORD.key(), null))
                        .setTimeout(Integer.parseInt(properties.getOrDefault(RedisOptions.TIMEOUT.key(), String.valueOf(RedisOptions.TIMEOUT.defaultValue()))));
                break;
            case RedisOptions.MODE_SENTINEL:
                builder = RedisConnectionOptions.withSentinel()
                        .setSentinelMaster(properties.get(RedisOptions.SENTINEL_MASTER.key()))
                        .setSentinelNodes(properties.get(RedisOptions.SENTINEL_NODES.key()))
                        .setSoTimeout(Integer.parseInt(properties.getOrDefault(RedisOptions.SO_TIMEOUT.key(), String.valueOf(RedisOptions.SO_TIMEOUT.defaultValue()))))
                        .setDatabase(Integer.parseInt(properties.getOrDefault(RedisOptions.DATABASE.key(), String.valueOf(RedisOptions.DATABASE.defaultValue()))));

                break;
            case RedisOptions.MODE_CLUSTER:
                builder =  RedisConnectionOptions.withCluster()
                        .setClusterNodes(properties.get(RedisOptions.CLUSTER_NODES.key()))
                        .setMaxRedirection(Integer.parseInt(properties.getOrDefault(RedisOptions.CLUSTER_MAX_REDIRECTIONS.key(), String.valueOf(RedisOptions.CLUSTER_MAX_REDIRECTIONS.defaultValue()))));
                break;
            default:
                throw new RedisUnsupportedException("Unsupported redis mode " + mode);
        }
        return builder.setPassword(properties.get(RedisOptions.PASSWORD.key()))
                .setTimeout(Integer.parseInt(properties.get(RedisOptions.TIMEOUT.key())))
                .setMaxTotal(Integer.parseInt(properties.get(RedisOptions.MAX_TOTAL.key())))
                .build();
    }

    public static RedisConnectionOptions getConnectorOptions(ReadableConfig config) {
        final String mode = config.get(RedisOptions.MODE);
        final Builder builder;
        switch (config.get(RedisOptions.MODE)) {
            case RedisOptions.MODE_SINGLE:
                builder = RedisConnectionOptions.withSingle()
                        .setHost(config.get(RedisOptions.HOST))
                        .setPort(config.get(RedisOptions.PORT))
                        .setDatabase(config.get(RedisOptions.DATABASE))
                        .setPassword(config.get(RedisOptions.PASSWORD))
                        .setTimeout(config.get(RedisOptions.TIMEOUT));
                break;
            case RedisOptions.MODE_SENTINEL:
                builder = RedisConnectionOptions.withSentinel()
                        .setSentinelMaster(config.get(RedisOptions.SENTINEL_MASTER))
                        .setSentinelNodes(config.get(RedisOptions.SENTINEL_NODES))
                        .setSoTimeout(config.get(RedisOptions.SO_TIMEOUT))
                        .setDatabase(config.get(RedisOptions.DATABASE));

                break;
            case RedisOptions.MODE_CLUSTER:
                builder =  RedisConnectionOptions.withCluster()
                        .setClusterNodes(config.get(RedisOptions.CLUSTER_NODES))
                        .setSoTimeout(config.get(RedisOptions.SO_TIMEOUT))
                        .setMaxRedirection(config.get(RedisOptions.CLUSTER_MAX_REDIRECTIONS));
                break;
            default:
                throw new RedisUnsupportedException("Unsupported redis mode " + mode);
        }
        return builder.setPassword(config.get(RedisOptions.PASSWORD))
                .setTimeout(config.get(RedisOptions.TIMEOUT))
                .setMaxTotal(config.get(RedisOptions.MAX_TOTAL))
                .build();
    }
}
