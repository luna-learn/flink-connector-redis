package org.apache.flink.connector.redis.container;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.connector.redis.RedisUnsupportedException;
import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.config.RedisOptions;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.commands.JedisCommands;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * RedisBaseContainer
 *
 * @author Liu Yang
 * @date 2023/1/16 8:50
 */
public class RedisClusterContainer extends RedisBaseContainer{

    private final RedisConnectionOptions options;
    private JedisCluster cluster;

    public RedisClusterContainer(RedisConnectionOptions options) {
        this.options = options;
    }

    private JedisCluster getResource() {
        return cluster;
    }

    @Override
    public void open() throws Exception {
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(options.getMaxTotal());
        poolConfig.setMaxIdle(options.getMaxIdle());
        poolConfig.setMinIdle(options.getMinIdle());
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        if (Objects.equals(options.getMode(), RedisOptions.MODE_CLUSTER)) {
            Set<HostAndPort> clusterNodes = Arrays
                    .stream(options.getClusterNodes().split(","))
                    .map(e -> {
                        if (e == null || e.length() == 0) {
                            return null;
                        }
                        int a = e.indexOf(":");
                        if (a == -1) {
                            return new HostAndPort(e, options.getPort());
                        } else {
                            return new HostAndPort(e.substring(0, a), Integer.parseInt(e.substring(a + 1)));
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
            cluster = new JedisCluster(clusterNodes,
                    options.getTimeout(),
                    options.getSoTimeout(),
                    3,
                    options.getPassword(),
                    poolConfig);
            // 设置 isClosed = false 表示 container 准备就绪
            this.isClosed = false;
        } else {
            throw new RedisUnsupportedException("RedisClusterContainer unsupported " +
                    "redis mode " + options.getMode());
        }


    }

    @Override
    public void close() {
        this.isClosed = true;
        if (cluster != null) {
            cluster.close();
        }
    }

    @Override
    protected JedisCommands getCommander() {
        return getResource();
    }

    public RedisConnectionOptions getOptions() {
        return options;
    }

    public Transaction multi() {
        return null;
    }

}
