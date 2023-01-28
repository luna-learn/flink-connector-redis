# flink-connector-redis

版本: 1.14.3  
作者: luna-learn  
更新时间: 2023/1/28 18:25
支持 flink 版本: 1.14.3

说明：  
(1) 基于 DynamicTableSourceFactory、DynamicTableSinkFactory 接口实现 Redis 读写。  
(2) 目前支持 string, hash, stream 三种 Redis 数据结构类型。  
(3) 可以通过 flink-sql 的方式映射 Redis 数据, 且支持 lookup, scan 和 stream 数据处理方式。

后续有时间会慢慢完善功能，有问题可以留言讨论。

## Flink sql 示例

```sql
CREATE CATALOG REDIS WITH ('type' = 'generic_in_memory');

CREATE DATABASE IF NOT EXISTS REDIS.DW;

CREATE TABLE REDIS.DW.EXAMPLE_TABLE
(
  `KEY`       VARCHAR(100)   COMMENT '键'
, `VALUE`     ROW<
    SCORE       DECIMALL(13,2) COMMENT '得分'
  , CREATE_DATE DATE           COMMENT '创建日期'
  , UPDATE_TIME TIMESTAMP(3)   COMMENT '更新时间'
  > COMMENT '值'
, PRIMARY KEY (`KEY`) NOT ENFORCED
) COMMENT 'REDIS建表示例表'
WITH (
  'connector' = 'redis'
, 'mode' = 'single'
, 'host' = 'localhost'
, 'port' = '6379'
, 'password' = '******'
, 'type' = 'hash'
, 'additional-key' = 'EXAMPLE_TABLE'
);

InSERT INTO REDIS.DW.EXAMPLE_TABLE
SELECT
  CAST('测试主键' AS VARCHAR(100)) AS `KEY`
, ROW( 
    CAST(99.99 AS DECIMAL(13,2))
  , CURRENT_DATE
  , CURRENT_TIMESTAMP
 ) AS `VALUE`
;
```
建表注意事项：  
（1）表只能有两个字段，第一个字段必需是主键字段，第二个字段是数据字段；  
（2）当 type = string 时，主键字段必须为STRING类型，数据字段目前支持基本数据类型和ROW复合数据类型；
（3）当 type = hash 时，主键字段必须为STRING类型，数据字段目前仅支持ROW复合数据类型；
（4）当 type = stream 时，必须设置 additional-key，字段格式与 hash 一致。

## Redis Options 说明
具体可以参看: org.apache.flink.connector.redis.config.RedisOptions  
默认值并不一定是合理的，可根据实际需求和使用经验来设置。

| Option                  |Type| Default   | Note                                                                                                        |
|-------------------------|---|-----------|-------------------------------------------------------------------------------------------------------------|
| connector               |string| redis     | 必选, 必须设置为 redis                                                                                             |
| format                  |string| json      | 可选, 目前仅支持json，可对RedisFormatter进行扩展                                                                          |
| mode                    |string| single    | 可选, 可从以下三项中选择: <br>(1) single<br>(2)sentinel<br>(3)cluster<br>其中,single为单机模式, sentinel为哨兵主从模式, cluster为集群模式 |
| host                    |string| localhost | 可选, single模式下的主机名                                                                                           |
| port                    |int| 6379      | 可选, single模式下端口                                                                                             |
| timeout                 |int| 2000      | 可选, 连接超时时间，单位为毫秒                                                                                            |
| database                |int| 0         | 可选, single 模式和 sentinel模式下的 Redis db 编号                                                                     |
| password                |string|           | 可选, Redis 密码, 默认为空值                                                                                         |
| type                    |string| hash      | 可选, Redis数结储存类型, 目前支持 string, hash, stream                                                                  |
| additional-key          |string| default   | 可选, 附着键，当 type=stream，必须设置该值                                                                                |
| maxTotal                |int| 8         | 可选, 最大连接数                                                                                                   |
| maxIdle                 |int| 8         | 可选, 最大空闲连接数                                                                                                 |
| minIdle                 |int| 8         | 可选, 最小空闲连接数                                                                                                 |
| sentinel.master         |string|           | 可选, sentinel模式下哨兵master节点, 格式为 (host or ip):port, 多个节点之间用","分隔                                              |
| sentinel.nodes          |string|           | 可选, sentinel模式下redis节点, 格式为 (host or ip):port, 多个节点之间用","分隔                                                 |
| so-timeout              |int| 3000      | 可选, sentinel模式和cluster模式下侍候超时时间，单位为毫秒                                                                       |
| cluster.nodes           |string|           | 可选, cluster模式下redis节点, 格式为 (host or ip):port, 多个节点之间用","分隔                                                  |
| cluster.redirection.max |int| 1         | 可选, cluster模式重定向最大次数                                                                                        |
| lookup.cache.max-size   |int| -1        | 可选, lookup 缓存最大数量, -1 表示不使用缓存                                                                               |
| lookup.cache.expire-ms  |int| -1        | 可选, lookup 缓存过期时间                                                                                           |
| lookup.retry.max-times  |int| 1         | 可选, lookup 重试次数                                                                                             |
| sink.key-ttl            |int| -1        | 可选, 使用 redis sink 写入数据时, key 对应的数据生命周期, 单位为毫秒, -1 表示不过期                                                     |                     |
| stream.group-name       |string|           | 可选, stream 消费者组名称, 为空表示不使用消费者组进行消费, 参考 https://redis.io/docs/data-types/streams-tutorial/                   |
| stream.entry-id         |string|           | 可选, stream 消费者开始入口标识, 格式为"时间戳-数字序号", 参考 https://redis.io/docs/data-types/streams-tutorial/                  |


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