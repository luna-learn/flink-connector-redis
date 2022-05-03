package org.apache.flink.connector.redis.table.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.RedisSinkException;
import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisSinkOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisFormatter;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;

public class RedisSink<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

    private final RedisConnectorOptions connectorOptions;
    private final RedisSinkOptions sinkOptions;
    private final RedisMapper redisMapper;

    private final String[] fieldNames;
    private final TypeInformation<?>[] fieldTypes;
    private final DataType[] dataTypes;
    private final LogicalType[] logicalTypes;
    private final String additionalKey;
    private final Integer ttl;
    private final RedisFormatter formatter;

    private final HashMap<String, RowData> buffer = new HashMap<>();

    private final int fieldNum;
    private final String primaryKey;
    private int primaryKeyIndex = 0;

    private RedisContainer redisContainer;


    public RedisSink(RedisConnectorOptions connectorOptions, RedisSinkOptions sinkOptions, RedisMapper redisMapper) {
        this.connectorOptions = connectorOptions;
        this.sinkOptions = sinkOptions;
        this.redisMapper = redisMapper;

        this.fieldNames = redisMapper.getFieldNames();
        this.fieldTypes = redisMapper.getFieldTypes();
        this.dataTypes = redisMapper.getDataTypes();
        this.additionalKey = redisMapper.getAdditionalKey();
        this.ttl = redisMapper.getKeyTtl();

        this.primaryKey = redisMapper.getPrimaryKey();
        this.fieldNum = fieldNames.length;
        for (int i=0; i<fieldNum; i++) {
            if (Objects.equals(primaryKey, fieldNames[i])) {
                this.primaryKeyIndex = i;
                break;
            }
        }
        this.logicalTypes = new LogicalType[fieldNum];
        for (int i=0; i<fieldNum; i++) {
            this.logicalTypes[i] = dataTypes[i].getLogicalType();
        }
        this.formatter = new RedisFormatter();
    }

    private void write(RowData row) {
        // 尝试使用事务, 注意：集群模式不支持
        Transaction transaction = redisContainer.multi();
        int length = row.getArity();
        String field = formatter.encode(row, primaryKeyIndex, logicalTypes[primaryKeyIndex]);
        // Redis 写入值需要注意 key 和 value 不能为空
        // 可通过 String.valueOf 将空值转换为 null 进行处理，用于对齐数据
        // 也可以不将 null 值写入redis，节省储存空间
        for (int i=0; i<length; i++) {
            String key = additionalKey + ":" + fieldNames[i];
            String value = String.valueOf(formatter.encode(row, i, logicalTypes[i]));
            try {
                if (transaction != null) {
                    transaction.hset(key, field, value);
                    if (ttl != null && ttl > 0) {
                        transaction.expire(key, ttl);
                    }
                } else {
                    redisContainer.hset(key, field, value);
                    if (ttl != null && ttl > 0) {
                        redisContainer.expire(key, ttl);
                    }
                }
            } catch (Exception e) {
                throw new RedisSinkException("Write data to redis failed" +
                        ", please check redis container and data" +
                        " (field=" + field + ", key=" + key + ", value=" + value + ")", e);
            }
        }
        if (transaction != null) {
            transaction.exec();
        }
    }

    private void handle(RowData row, boolean useBuffer) {
        int length = row.getArity();
        if (length != fieldNames.length) {
            throw new IllegalArgumentException("Input RowData length not equals fieldNames, " +
                    "required: " + fieldNames.length + ", " +
                    "provided: " + length);
        }
        if (useBuffer) {
            synchronized (buffer) {
                if (buffer.size() < 1000) {
                    String key = String.valueOf(row.getString(primaryKeyIndex));
                    buffer.put(key, row);
                } else {
                    flush();
                }
            }
        } else {
            write(row);
        }

    }

    private void flush() {
        synchronized (buffer) {
            if (buffer.size() == 0) {
                return;
            }
            buffer.forEach((field, row) -> write(row));
            buffer.clear();
        }
    }

    @Override
    public void invoke(IN input, Context context) throws Exception {
        if (input instanceof RowData) {
            handle((RowData) input, false);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        redisContainer = connectorOptions.getContainer();
        redisContainer.open();
    }

    @Override
    public void close() throws IOException {
        if (redisContainer != null) {
            flush();
            redisContainer.close();
        }
    }

}
