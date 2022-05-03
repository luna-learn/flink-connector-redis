# flink-connector-redis

版本: 1.0  
作者: luna-learn  
支持 flink 版本: 1.13.6

说明：  
(1) 基于 DynamicTableSourceFactory、DynamicTableSinkFactory 接口实现 Redis 读写。  
(2) 目前支持 Redis Hash 模式，读写功能是基于 HGET 和 HSET 来实现，其余模式还有待开发。  
(3) 可以通过 flink-sql 的方式映射 Redis 数据, Hash 模式下，使用的是列式存储模式。  
(4) 目前并未实现 checkpoint 相关功能，有待后续开发。

后续有时间会慢慢完善功能，有问题可以留言讨论。

## Flink sql 示例

```sql
CREATE CATALOG REDIS WITH ('type' = 'generic_in_memory');

CREATE DATABASE IF NOT EXISTS REDIS.DW;

CREATE TABLE REDIS.DW.EXAMPLE_TABLE
(
    `KEY`       VARCHAR(100)   COMMENT '主键'
    , `VALUE`     VARCHAR(50)    COMMENT '值'
    , SCORE       DECIMALL(13,2) COMMENT '得分'
    , CREATE_DATE DATE           COMMENT '创建日期'
    , UPDATE_TIME TIMESTAMP(3)   COMMENT '更新时间'
    , PRIMARY KEY (KEY) NOT ENFORCED
) COMMENT 'REDIS.DW.EXAMPLE_TABLE.示例表'
WITH (
'connector' = 'redis',
'mode' = 'single',
'host' = 'localhost',
'port' = '6379',
'password' = '******'
'additional-key' = 'DW:EXAMPLE_TABLE',
'lookup.cache.max-size' = '10',
'lookup.cache.expire-ms' = '60000',
'scan.is-bounded' = 'true'
);

InSERT INTO REDIS.DW.EXAMPLE_TABLE
SELECT
    CAST('测试主键' AS VARCHAR(100))
     , CAST('测试值' AS VARCHAR(50))
     , CAST(99.99 AS DECIMAL(13,2))
     , CURRENT_DATE
     , CURRENT_TIMESTAMP
;
```

## Redis Options 说明
具体可以参看: org.apache.flink.connector.redis.config.RedisOptions  
默认值并不一定是合理的，可根据实际需求和使用经验来设置。

|Option|Type|Default|Note|
|---|---|---|---|
|connector|string|redis|必选, 必须设置为 redis|
|mode|string|single|可选, 可从以下三项中选择: <br>(1) single<br>(2)sentinel<br>(3)cluster<br>其中,single为单机模式, sentinel为哨兵主从模式, cluster为集群模式|
|host|string|localhost|可选, single模式下的主机名|
|port|int|6379|可选, single模式下端口|
|timeout|int|2000|可选, 连接超时时间，单位为毫秒|
|database|int|0|可选, single模式和sentinel模式下的Redis db 编号|
|password|string| |可选, Redis 密码, 默认为空值 |
|additional-key|string|default|可选, 附着键，Hash列存储模式下，字段会附着在该键值下|
|maxTotal|int|8|可选, 最大连接数 |
|maxIdle|int|8|可选, 最大空闲连接数 |
|minIdle|int|8|可选, 最小空闲连接数|
|sentinel.master|string| |可选, sentinel模式下哨兵master节点, 格式为 (host or ip):port, 多个节点之间用","分隔 |
|sentinel.nodes|string| |可选, sentinel模式下redis节点, 格式为 (host or ip):port, 多个节点之间用","分隔 |
|so-timeout|int|3000|可选, sentinel模式和cluster模式下侍候超时时间，单位为毫秒 |
|cluster.nodes|string| |可选, cluster模式下redis节点, 格式为 (host or ip):port, 多个节点之间用","分隔 |
|cluster.redirection.max|int|1|可选, cluster模式重定向最大次数 |
|lookup.cache.max-size|int|-1|可选, lookup 缓存最大数量, -1 表示不使用缓存 |
|lookup.cache.expire-ms|int|-1|可选, lookup 缓存过期时间 |
|lookup.retry.max-times|int|1|可选, lookup 重试次数 |
|sink.key-ttl|int|-1|可选, 使用 redis sink 写入数据时, key 对应的数据生命周期, 单位为毫秒, -1 表示不过期 |
|scan.is-bounded|boolean|false|可选, scan 是否有界, 如果设置为无界scan时, 会自动监测 redis 数据变化, 当 redis 数据发生变化后, 就会触发流事件, 目前属于试验性质的功能 |


## Flink Sql 数据类型支持说明
|Type|Java Prototype|Supported|Note|
|---|---|---|---|
|ARRAY<?>|T[]|是|数组, 示例 ARRAY&lt;STRING&gt;|
|BIGINT|Long|是| |
|BOOLEAN|Boolean|是| |
|CHAR|String|是| |
|Decimal|BigDecimal|是| |
|Date|LocalDate|是| |
|DOUBLE|Double|是| |
|FLOAT|Float|是| |
|INT|Integer|是| |
|MULTISET|Map|否|映射表，目前暂不支持|
|SMALLINT|Short|是| |
|STRING|String|是| |
|TIME|LocalTime|是| |
|TIMESTAMP|LocalDateTime|是|
|VARBINARY|byte[]|是|字节数组, 会转换为 Hex 进行存储, 取出时再通过 Hex 解码|
|VARCHAR|String|是| |