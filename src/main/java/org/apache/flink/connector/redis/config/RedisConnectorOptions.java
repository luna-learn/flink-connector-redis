package org.apache.flink.connector.redis.config;

import org.apache.flink.connector.redis.UnsupportedRedisException;
import org.apache.flink.connector.redis.container.RedisClusterContainer;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.container.RedisPoolContainer;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisConnectorOptions implements Serializable {

    private final static Map<RedisConnectorOptions, RedisContainer> CONTAINER_POOL = new ConcurrentHashMap<>();

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


    private RedisConnectorOptions() {

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
        if (CONTAINER_POOL.containsKey(this)) {
            return CONTAINER_POOL.get(this);
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
                throw new UnsupportedRedisException("Unsupported redis mode " + mode);
        }
        CONTAINER_POOL.put(this, container);
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

    public static class Builder {
        RedisConnectorOptions options;

        private Builder() {
            options = new RedisConnectorOptions();
            options.maxTotal = 8;
            options.maxIdle = 8;
            options.minIdle = 0;
            options.timeout = 2000;
            options.database = 0;
        }

        public Builder setPassword(String password) {
            options.password = password;
            return this;
        }

        public Builder setTimeout(int timeout) {
            options.timeout = timeout;
            return this;
        }

        public Builder setMaxTotal(int maxTotal) {
            options.maxTotal = maxTotal;
            return this;
        }

        public Builder setMaxIdle(int maxIdle) {
            options.maxIdle = maxIdle;
            return this;
        }

        public Builder setMinIdle(int minIdle) {
            options.minIdle = minIdle;
            return this;
        }

        public RedisConnectorOptions build() {
            return options;
        }

    }

    public static class SingleBuilder extends Builder {

        private SingleBuilder() {
            super();
            options.mode = RedisOptions.MODE_SINGLE;
            options.host = "localhost";
            options.port = 6397;
            options.password = null;
        }

        public SingleBuilder setHost(String host) {
            options.host = host;
            return this;
        }

        public SingleBuilder setPort(int port) {
            options.port = port;
            return this;
        }

        public SingleBuilder setDatabase(int database) {
            options.database = database;
            return this;
        }
    }

    public static class SentinelBuilder extends Builder {
        private SentinelBuilder() {
            super();
            options.mode = RedisOptions.MODE_SENTINEL;
            options.soTimeout = 2000;
        }

        public SentinelBuilder setSentinelMaster(String sentinelMaster) {
            options.sentinelMaster = sentinelMaster;
            return this;
        }

        public SentinelBuilder setSentinelNodes(String sentinelNodes) {
            options.sentinelNodes = sentinelNodes;
            return this;
        }

        public SentinelBuilder setDatabase(int database) {
            options.database = database;
            return this;
        }

        public SentinelBuilder setSoTimeout(int soTimeout) {
            options.soTimeout = soTimeout;
            return this;
        }
    }

    public static class ClusterBuilder extends Builder {
        private ClusterBuilder() {
            super();
            options.mode = RedisOptions.MODE_CLUSTER;
        }

        public ClusterBuilder setClusterNodes(String clusterNodes) {
            options.clusterNodes = clusterNodes;
            return this;
        }

        public ClusterBuilder setMaxRedirection(int maxRedirection) {
            options.maxRedirection = maxRedirection;
            return this;
        }

        public ClusterBuilder setSoTimeout(int soTimeout) {
            options.soTimeout = soTimeout;
            return this;
        }
    }

}
