package org.apache.flink.connector.redis.container;

import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamGroupInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RedisContainer {

    /**
     * 打开
     * *
     * @throws Exception 失败时抛出异常信息
     */
    void open() throws Exception;

    /**
     * 是否已关闭
     * @return 返回true表示已关闭，返回false表示已打开
     */
    boolean isClosed();

    /**
     * 关闭
     */
    void close();

    /**
     * 获取配置连接配置
     *
     * @return 返回连接配置实例对象
     */
    RedisConnectionOptions getOptions();

    /**
     * 开始一个事务
     * <p>
     * 可以通过调用返回的事务实例方法实现commit和rollback功能。
     * 单机或者哨兵主从模式支持，集群模式不支持
     * </p>
     * @return 返回事务实例
     */
    Transaction multi();

    /**
     * 设置键的过期时间
     *
     * @param key     键
     * @param seconds 过期时间，单位：秒。
     */
    void expire(String key, long seconds);

    /**
     * 获取键的过期时间
     *
     * @return 返回键的过期时间。返回-1表示持久化。
     */
    long ttl(String key);

    boolean exists(String key);

    long exists(String... keys);

    void del(String key);

    String get(String key);

    Set<String> keys(String pattern);

    /**
     * 扫描
     *
     * @param cursor 游标。初始游标为 0。
     * @param params 扫描参数。
     *               MATCH [pattern] 为匹配表达式，比如：he*, wo*d；
     *               COUNT [n] 指定此次扫描返回的最大结果数量。
     * @param type   类型。
     *               与 TYPE [key] 返回的键类型名称是相同的，
     *               但有一些键的值是采用Redis其他Redis类型实现的，
     *               例如GeoHashes, HyperLogLogs, bitmap和bitfield，并不能完全等同。
     * @return 返回扫描结果，其中有游标和匹配的键列表。
     *               返回的游标为0时，表示扫描结束；
     *               返回的匹配的键列表也有可能是空集。
     */
    ScanResult<String> scan(String cursor, ScanParams params, String type);

    /**
     * 扫描
     *
     * @param cursor 游标。初始游标为 0
     * @param params 扫描参数。
     *               MATCH [pattern] 为匹配表达式，比如：he*, wo*d；
     *               COUNT [n] 指定此次扫描返回的最大结果数量。
     * @return 返回扫描结果，其中有游标和匹配的键列表。
     *               返回的游标为0时，表示扫描结束；
     *               返回的匹配的键列表也有可能是空集。
     */
    ScanResult<String> scan(String cursor, ScanParams params);

    /**
     * 设置键值
     *
     * @param key   键
     * @param value 值
     */
    void set(String key, String value);

    /**
     * 设置键值
     * <p>
     * 等同于 SET if not exits。即如果存在键值测不操作，不存在键存则设置键值。
     * </p>
     * @param key   键
     * @param value 值
     */
    void setnx(String key, String value);

    /**
     * 批量设置键值
     *
     * @param keysvalues example: key1 "Hello" key2 "World" ......
     */
    void mset(String... keysvalues);

    /**
     * 获取键类型
     *
     * @param key 键
     * @return 返回键类型。可返回以下类型：string, list, set, zset, hash, stream。
     */
    String type(String key);

    void hdel(String key, String field);

    String hget(String key, String field);

    long hlen(String key);

    Set<String> hkeys(String key);

    ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params);

    default ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        return hscan(key, cursor, new ScanParams());
    }

    void hset(String key, String field, String value);

    void hset(String key, String field, String value, int ttl);

    List<String> hmget(String key, String... fields);

    void hmset(String key, Map<String, String> values);

    long xack(String key, String group, StreamEntryID... ids);

    /**
     * 添加 Stream 数据
     *
     * @param key 指定的 Stream Key
     * @param id  指定 Stream 实体标识，作用类似于 offset
     * @param hash 待添加的数据。类型为Hash
     * @return 返回 Stream 实体标识
     */
    StreamEntryID xadd(String key, StreamEntryID id, Map<String, String> hash);

    /**
     * 添加 Stream 数据
     *
     * @param key     指定的 Stream Key
     * @param params  指定 XADD 命令参数
     * @param hash 待添加的数据。类型为Hash
     * @return 返回 Stream 实体标识
     */
    StreamEntryID xadd(String key, XAddParams params, Map<String, String> hash);

    /**
     * 删除指定的 Stream Key
     * @param key 待删除的 Stream Key
     * @param ids 指定的 Stream 实体标识。在 xread、xreadgroup 等方法接收到的消息中有该标识。
     * @return 返回删除的消息数量
     */
    long xdel(String key, StreamEntryID... ids);

    /**
     * 删除消费组中指定的消费者
     *
     * @param key          指定的 Stream Key
     * @param groupName    指定的消费者组名
     * @param consumerName 指定的消费者名称
     * @return 返回1表示删除成功
     */
    long xgroupDelConsumer( String key, String groupName, String consumerName);

    /**
     * 创建 Stream 消费组
     * <p>
     * 参考：https://redis.io/commands/xgroup-create/
     * </p>
     * @param key        指定的 Stream key
     * @param groupName  指定组名
     * @param id         指定 Stream 实体标识。一般情况下填 StreamEntryID.LAST_ENTRY
     * @param makeStream 当 Stream key 不存在时是否创建流
     * @return 返回 OK 表示创建成功
     */
    String xgroupCreate(String key, String groupName, StreamEntryID id, boolean makeStream);

    /**
     * 删除 Stream 消费组
     *
     * @param key       指定的 Stream key
     * @param groupName 指定组名
     * @return 返回1表示删除成功
     */
    long xgroupDestroy(String key, String groupName);

    /**
     * 获取指定的 Stream key 相关的消费者组
     *
     * @param key 指定的 Stream key
     * @return 返回 Stream 消费者组信息列表
     */
    List<StreamGroupInfo> xinfoGroups(String key);


    String xgroupSetID(String key, String groupName, StreamEntryID id);

    /**
     * 获取指定的 Stream 数据长度
     *
     * @param key 指定的 Stream Key
     * @return 返回 Stream 数据长度
     */
    long xlen(String key);

    /**
     * 读取 Stream 数据
     *
     * @param xReadParams 指定的 ReadParams
     * @param streams     指定的 Stream，结构为 keyName -> StreamEntryID
     * @return 返回读取到的 StreamEntry
     */
    List<Map.Entry<String, List<StreamEntry>>> xread(XReadParams xReadParams, Map<String, StreamEntryID> streams);

    /**
     * 指定消费者组和消费者读取 Stream 数据
     *
     * @param groupName 指定消费组名称
     * @param consumer  指定消费者名称
     * @param xReadGroupParams 指定的 XReadGroupParams
     * @param streams     指定的 Stream，结构为 keyName -> StreamEntryID
     * @return 返回读取到的 StreamEntry
     */
    List<Map.Entry<String, List<StreamEntry>>> xreadGroup(String groupName, String consumer,
                                                          XReadGroupParams xReadGroupParams, Map<String, StreamEntryID> streams);

}