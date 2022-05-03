package org.apache.flink.connector.redis.table.source;

import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisFormatter;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class RedisUnboundTableSource extends RichParallelSourceFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisUnboundTableSource.class);

    private final RedisConnectorOptions connectorOptions;
    private final RedisSourceOptions sourceOptions;
    private final RedisMapper redisMapper;
    private RedisContainer redisContainer;
    private final String[] fieldNames;
    private final DataType[] dataTypes;
    private final LogicalType[] logicalTypes;
    private final RedisFormatter formatter;
    private final int fieldNum;
    private final String primaryKey;
    private int primaryKeyIndex = 0;
    private String cursor = "0";
    private String lastCursor = "0";
    private ScanResult<Map.Entry<String, String>> scanResult;
    private long dataCount = 0;
    private ScanParams scanParams;
    private List<Map.Entry<String, String>> scanBuffer;

    private final int cacheExpireMs;
    private final int cacheMaxSize;
    private final int maxRetryTimes;
    private final String additionalKey;
    private Cache<String, GenericRowData> cache;
    private boolean running = false;

    public RedisUnboundTableSource(RedisConnectorOptions connectorOptions,
                                   RedisSourceOptions sourceOptions,
                                   RedisMapper redisMapper) {
        this.connectorOptions = connectorOptions;
        this.sourceOptions = sourceOptions;
        this.redisMapper = redisMapper;
        this.additionalKey = redisMapper.getAdditionalKey();
        this.fieldNames = redisMapper.getFieldNames();
        this.dataTypes = redisMapper.getDataTypes();
        this.cacheExpireMs = sourceOptions.getCacheExpireMs();
        this.cacheMaxSize = sourceOptions.getCacheMaxSize();
        this.maxRetryTimes = sourceOptions.getMaxRetryTimes();

        this.fieldNum = fieldNames.length;
        for (int i=0; i<fieldNum; i++) {
            if (Objects.equals(redisMapper.getPrimaryKey(), fieldNames[i])) {
                this.primaryKeyIndex = i;
                break;
            }
        }

        this.primaryKey = redisMapper.getPrimaryKey() == null ?
                this.fieldNames[primaryKeyIndex] : redisMapper.getPrimaryKey();

        this.logicalTypes = new LogicalType[fieldNum];
        for (int i=0; i<fieldNum; i++) {
            this.logicalTypes[i] = dataTypes[i].getLogicalType();
        }
        this.formatter = new RedisFormatter();
    }

    private void trySleep(long mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void tryLoadDataFromRedis() {
        long count = redisContainer.hlen(additionalKey + ":" + primaryKey);
        if (count > 0 && count != dataCount) {
            LOG.info("Redis key " + additionalKey + ":" + primaryKey +
                    " length has benn changed (" + dataCount + " -> " + count + ")." );
            for(int i=0; i<maxRetryTimes; i++) {
                scanParams = new ScanParams()
                        .count(1);
                scanResult = redisContainer.hscan(additionalKey + ":" + primaryKey,
                        lastCursor,
                        scanParams);
                cursor = scanResult.getCursor();
                scanBuffer = scanResult.getResult();
                if (!"0".equals(cursor) || scanBuffer.size() > 0) {
                    lastCursor = cursor;
                    dataCount = count;
                    break;
                }
                // 如果未查到数据，进行短暂等待
                trySleep(1000);
            }
        } else if (count == 0) {
            LOG.info("Redis key " + additionalKey + ":" + primaryKey +
                    " length is 0, maybe key is empty." );
            trySleep(1000);
        } else {
            LOG.info("Redis key " + additionalKey + ":" + primaryKey +
                    " length has not change (" + count + ")." );
            trySleep(1000);
        }

    }

    public void open(Configuration parameters) {
        try{
            redisContainer = connectorOptions.getContainer();
            redisContainer.open();
            running = true;
            //
            tryLoadDataFromRedis();
            //
            if (cache == null && cacheExpireMs > 0 && cacheMaxSize > 0) {
                cache = CacheBuilder.newBuilder()
                        .recordStats()
                        .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                        .maximumSize(cacheMaxSize)
                        .build();
            }
        } catch (Exception e) {
            System.out.println("openInputFormat, " + e);
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws IOException {
        LOG.info("Redis sink close." );
        running = false;
        if (redisContainer != null) {
            redisContainer.close();
        }
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        while (running) {
            // 如果缓冲区没有数据，尝试从redis拉取数据
            if (scanBuffer == null || scanBuffer.size() <= 0) {
                tryLoadDataFromRedis();
            }
            if (scanBuffer == null || scanBuffer.size() <= 0) {
                sourceContext.markAsTemporarilyIdle();
                // 空闲等待
                trySleep(1000);
                continue;
            }
            GenericRowData row;
            String field = scanBuffer.remove(0).getKey(); // 从顶部删除
            String cacheKey = additionalKey + ":" + fieldNames[primaryKeyIndex] + ":" + field;
            if (cache != null) {
                row = cache.getIfPresent(cacheKey);
                if (row != null) {
                    sourceContext.collectWithTimestamp(row, System.currentTimeMillis());
                    continue;
                }
            }
            row = new GenericRowData(fieldNames.length);
            for (int i=0; i<fieldNum; i++) {
                String value = redisContainer.hget(additionalKey + ":" + fieldNames[i], field);
                row.setField(i, formatter.decode(value, logicalTypes[i]));
            }
            sourceContext.collectWithTimestamp(row, System.currentTimeMillis());
            if (cache != null) {
                cache.put(cacheKey, row);
            }
            if (!running) {
                return;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        if (redisContainer != null) {
            redisContainer.close();
        }
    }
}
