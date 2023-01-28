package org.apache.flink.connector.redis.container;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamGroupInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RedisBaseContainer
 *
 * @author Liu Yang
 * @date 2023/1/16 8:30
 */
public abstract class RedisBaseContainer implements RedisContainer, Serializable {

    protected boolean isClosed = true;

    protected abstract JedisCommands getCommander();

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void expire(String key, long seconds) {
        getCommander().expire(key, seconds);
    }

    @Override
    public boolean exists(String key) {
        return getCommander().exists(key);
    }

    @Override
    public long exists(String... keys) {
        return getCommander().exists(keys);
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
    public void setnx(String key, String value) {
        getCommander().setnx(key, value);
    }

    @Override
    public void mset(String... keysvalues) {
        getCommander().mset(keysvalues);
    }

    @Override
    public String get(String key) {
        return getCommander().get(key);
    }

    @Override
    public ScanResult<String> scan(String cursor, ScanParams params, String type) {
        return getCommander().scan(cursor, params, type);
    }

    @Override
    public ScanResult<String> scan(String cursor, ScanParams params) {
        return getCommander().scan(cursor, params);
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

    public long ttl(String key) {
        return getCommander().ttl(key);
    }

    @Override
    public String type(String key) {
        return getCommander().type(key);
    }

    @Override
    public long xack(String key, String group, StreamEntryID... ids) {
        return getCommander().xack(key, group, ids);
    }

    @Override
    public StreamEntryID xadd(String key, StreamEntryID id, Map<String, String> hash) {
        return getCommander().xadd(key, id, hash);
    }

    @Override
    public StreamEntryID xadd(String key, XAddParams params, Map<String, String> hash) {
        return getCommander().xadd(key, params, hash);
    }

    @Override
    public long xdel(String key, StreamEntryID... ids) {
        return getCommander().xdel(key, ids);
    }

    @Override
    public String xgroupCreate(String key, String groupName, StreamEntryID id, boolean makeStream) {
        return getCommander().xgroupCreate(key, groupName, id, makeStream);
    }

    @Override
    public long xgroupDelConsumer( String key, String groupName, String consumerName) {
        return getCommander().xgroupDelConsumer(key, groupName, consumerName);
    }

    @Override
    public long xgroupDestroy(String key, String groupName) {
        return getCommander().xgroupDestroy(key, groupName);
    }

    @Override
    public List<StreamGroupInfo> xinfoGroups(String key) {
        return getCommander().xinfoGroups(key);
    }

    @Override
    public long xlen(String key) {
        return getCommander().xlen(key);
    }

    @Override
    public List<Map.Entry<String, List<StreamEntry>>> xread(XReadParams xReadParams, Map<String, StreamEntryID> streams) {
        return getCommander().xread(xReadParams, streams);
    }

    @Override
    public List<Map.Entry<String, List<StreamEntry>>> xreadGroup(String groupName, String consumer,
                                                          XReadGroupParams xReadGroupParams, Map<String, StreamEntryID> streams){
        return getCommander().xreadGroup(groupName, consumer, xReadGroupParams, streams);
    }

    @Override
    public String xgroupSetID(String key, String groupName, StreamEntryID id) {
        return getCommander().xgroupSetID(key, groupName, id);
    }

}
