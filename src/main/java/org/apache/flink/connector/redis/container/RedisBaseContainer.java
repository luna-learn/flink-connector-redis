package org.apache.flink.connector.redis.container;

import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class RedisBaseContainer implements RedisContainer {

    protected abstract JedisCommands getCommander();

    public void expire(String key, long seconds) {
        getCommander().expire(key, seconds);
    }

    @Override
    public void del(String key) {
        getCommander().del(key);
    }

    @Override
    public void set(String key, String value) {
        getCommander().set(key, value);
    }

    @Override
    public String get(String key) {
        return getCommander().get(key);
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
        return getCommander().hscan(key, cursor, params);
    }

    public long hlen(String key) {
        return getCommander().hlen(key);
    }

    @Override
    public void hdel(String key, String field) {
        getCommander().hdel(key, field);
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        return  getCommander().hscan(key, cursor);
    }

    @Override
    public void hset(String key, String field, String value) {
        getCommander().hset(key, field, value);
    }

    @Override
    public void hset(String key, String field, String value, int ttl) {
        getCommander().hset(key, field, value);
        getCommander().expire(key, ttl);
    }

    public void hmset(String key, Map<String, String> values) {
        getCommander().hmset(key, values);
    }

    @Override
    public String hget(String key, String field) {
        return getCommander().hget(key, field);
    }

    public List<String> hmget(String key, String... fields) {
       return getCommander().hmget(key, fields);
    }

    @Override
    public Set<String> keys(String pattern) {
        return getCommander().keys(pattern);
    }

    @Override
    public Set<String> hkeys(String key) {
        return getCommander().hkeys(key);
    }

    @Override
    public String type(String key) {
        return getCommander().type(key);
    }
}
