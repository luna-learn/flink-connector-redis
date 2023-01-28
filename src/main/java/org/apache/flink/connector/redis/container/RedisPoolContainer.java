package org.apache.flink.connector.redis.container;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.connector.redis.RedisUnsupportedException;
import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.config.RedisOptions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.commands.JedisCommands;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisPoolContainer extends RedisBaseContainer {

    private final RedisConnectionOptions options;
    private JedisPool jedisPool;
    private JedisSentinelPool sentinelPool;
    private Jedis jedis;

    public RedisPoolContainer(RedisConnectionOptions options) {
        this.options = options;
    }

    private Jedis getResource() {
        if (jedis == null) {
            switch (options.getMode()) {
                case RedisOptions.MODE_SINGLE:
                    jedis = jedisPool.getResource();
                    break;
                case RedisOptions.MODE_SENTINEL:
                    jedis = sentinelPool.getResource();
                    break;
                default:
                    throw new RedisUnsupportedException("RedisPoolContainer unsupported " +
                            "redis mode " + options.getMode());
            }
        }
        return jedis;
    }

    @Override
    public void open() throws Exception {
        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(options.getMaxTotal());
        poolConfig.setMaxIdle(options.getMaxIdle());
        poolConfig.setMinIdle(options.getMinIdle());
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);

        switch(options.getMode()) {
            case RedisOptions.MODE_SINGLE:
                jedisPool = new JedisPool(poolConfig,
                        options.getHost(),
                        options.getPort(),
                        options.getTimeout(),
                        options.getPassword(),
                        options.getDatabase());
                break;
            case RedisOptions.MODE_SENTINEL:
                Set<String> sentinelNodes = Arrays
                        .stream(options.getSentinelNodes().split(","))
                        .collect(Collectors.toSet());
                sentinelPool = new JedisSentinelPool(options.getSentinelMaster(),
                        sentinelNodes,
                        poolConfig,
                        options.getSoTimeout(),
                        options.getTimeout(),
                        options.getPassword(),
                        options.getDatabase());
                break;
            default:
                throw new RedisUnsupportedException("RedisPoolContainer unsupported " +
                        "redis mode " + options.getMode());
        }
        // 设置 isClosed = false 表示 container 准备就绪
        this.isClosed = false;
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
        if (sentinelPool != null) {
            sentinelPool.close();
        }
    }

    @Override
    protected JedisCommands getCommander() {
        return getResource();
    }

    @Override
    public RedisConnectionOptions getOptions() {
        return options;
    }

    @Override
    public Transaction multi() {
        return getResource().multi();
    }
}
