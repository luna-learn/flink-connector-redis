package org.apache.flink.connector.redis.table.source;


import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisFormatter;
import org.apache.flink.connector.redis.mapper.RedisQueryMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class RedisLookupTableFunction extends TableFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupTableFunction.class);

    private final RedisConnectorOptions connectorOptions;
    private final RedisSourceOptions sourceOptions;
    private final RedisQueryMapper redisMapper;
    private RedisContainer redisContainer;
    private final RedisFormatter formatter;
    private final String[] fieldNames;
    private final DataType[] dataTypes;
    private final LogicalType[] logicalTypes;

    private final int fieldNum;
    private final int queryPrimaryIndex;
    private final String[] queryKeys;
    private final Integer[] queryIndexes;
    private final String primaryKey;
    private int primaryKeyIndex = 0;
    private String cursor = "0";
    private ScanResult<Map.Entry<String, String>> scanResult;
    private ScanParams scanParams;
    private List<Map.Entry<String, String>> scanBuffer;

    private final int cacheExpireMs;
    private final int cacheMaxSize;
    private final int maxRetryTimes;
    private final String additionalKey;
    private Cache<String, GenericRowData> cache;

    public RedisLookupTableFunction(RedisConnectorOptions connectorOptions,
                                    RedisSourceOptions sourceOptions,
                                    RedisQueryMapper redisMapper) {
        this.connectorOptions = connectorOptions;
        this.sourceOptions = sourceOptions;
        this.redisMapper = redisMapper;

        this.fieldNames = redisMapper.getFieldNames();
        this.dataTypes = redisMapper.getDataTypes();
        this.additionalKey = redisMapper.getAdditionalKey();
        this.cacheExpireMs = sourceOptions.getCacheExpireMs();
        this.cacheMaxSize = sourceOptions.getCacheMaxSize();
        this.maxRetryTimes =  sourceOptions.getMaxRetryTimes();

        this.queryKeys = redisMapper.getQueryKeys();
        this.primaryKey = redisMapper.getPrimaryKey();
        this.primaryKeyIndex = Arrays.asList(fieldNames).indexOf(primaryKey);
        this.queryPrimaryIndex = Arrays.asList(queryKeys).indexOf(primaryKey);
        this.queryIndexes = Arrays.stream(queryKeys)
                .map(e -> Arrays.asList(fieldNames).indexOf(e))
                .toArray(Integer[]::new);
        this.fieldNum = fieldNames.length;
        this.logicalTypes = Arrays.stream(dataTypes)
                .map(DataType::getLogicalType)
                .toArray(LogicalType[]::new);
        this.formatter = new RedisFormatter();
    }

    public void open(FunctionContext context) throws Exception {
        redisContainer = connectorOptions.getContainer();
        redisContainer.open();
        //
        if (cache == null && cacheExpireMs > 0 && cacheMaxSize > 0) {
            cache = CacheBuilder.newBuilder()
                    .recordStats()
                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(cacheMaxSize)
                    .build();
        }
    }


    private GenericRowData restore(String key) {
        if (cache != null && key != null) {
            return cache.getIfPresent(key);
        }
        return null;
    }

    private void store(String key, GenericRowData row) {
        if (cache != null && row != null) {
            cache.put(key, row);
        }
    }

    private GenericRowData lookup(String field, GenericRowData input) {
        // 尝试从缓存中拉取数据
        GenericRowData row = restore(field);
        if (row != null) {
            return row;
        }
        row = new GenericRowData(fieldNum);
        boolean exists = false;
        for (int i=0; i<maxRetryTimes; i++) {
            try {
                for(int j=0; j<fieldNum; j++) {
                    try {
                        String value = redisContainer.hget(additionalKey + ":" + fieldNames[j],
                                field);
                        row.setField(j, formatter.decode(value, logicalTypes[j]));
                    } catch (Exception e) {
                        throw e;
                    }
                }
                break;
            } catch (Exception e) {
                if (i >= this.maxRetryTimes - 1) {
                    throw new RuntimeException("Execution of redis lookup failed.", e);
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
        return row;
    }

    public void eval(Object... values){
        if (values == null) {
            return;
        }
        if (values.length != queryKeys.length) {
            throw new IllegalArgumentException("Input parameter number different from keys" +
                    "[" + String.join(",", queryKeys) + "], " +
                    "require " + queryKeys.length + " values, but given " + values.length + ".");
        }
        // 传和的参数组装为 GenericRowData
        final GenericRowData input = GenericRowData.of(values);
        // 获取主键值
        String field = String.valueOf(formatter.encode(input, queryPrimaryIndex, logicalTypes[primaryKeyIndex]));
        // 获取数据
        final GenericRowData row = lookup(field, input);
        // 是否匹配值
        boolean matched = Arrays.stream(queryIndexes)
                .allMatch(e -> Objects.equals(row.getField(e), values[e]));
        if (matched) {
            collect(row);
            // 主键值补回
            if (row.getField(primaryKeyIndex) == null) {
                row.setField(primaryKeyIndex, values[queryPrimaryIndex]);
            }
            // 缓存数据
            store(field, row);
        }
    }

    public void close() throws Exception {
        if (redisContainer != null) {
            redisContainer.close();
        }
    }
}
