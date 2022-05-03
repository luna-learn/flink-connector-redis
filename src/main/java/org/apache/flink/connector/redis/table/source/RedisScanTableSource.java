package org.apache.flink.connector.redis.table.source;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisFormatter;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
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

public class RedisScanTableSource extends RichInputFormat<RowData, InputSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupTableFunction.class);

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
    private ScanResult<Map.Entry<String, String>> scanResult;
    private ScanParams scanParams;
    private List<Map.Entry<String, String>> scanBuffer;

    private final int cacheExpireMs;
    private final int cacheMaxSize;
    private final int maxRetryTimes;
    private final String additionalKey;
    private Cache<String, GenericRowData> cache;

    public RedisScanTableSource(RedisConnectorOptions connectorOptions,
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

    private void tryLoadDataFromRedis() {
        for(int i=0; i<maxRetryTimes; i++) {
            scanParams = new ScanParams()
                    .count(1);
            scanResult = redisContainer.hscan(additionalKey + ":" + fieldNames[primaryKeyIndex],
                    cursor,
                    scanParams);
            cursor = scanResult.getCursor();
            scanBuffer = scanResult.getResult();
            if (!"0".equals(cursor) || scanBuffer.size() > 0) {
                break;
            }
            // 如果未查到数据，进行短暂等待
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void openInputFormat() {
        try{
            redisContainer = connectorOptions.getContainer();
            redisContainer.open();
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
    public void closeInputFormat() {
        if (redisContainer != null) {
            redisContainer.close();
        }
    }

    @Override
    public void configure(Configuration configuration) {
        // do nothing here
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return baseStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int i) throws IOException {
        return new GenericInputSplit[] {new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        // do nothing
    }

    @Override
    public boolean reachedEnd() throws IOException {
        // return keySet == null || !keySet.hasNext();
        return scanResult == null || scanBuffer == null || ("0".equals(cursor) && scanBuffer.size() == 0);
    }

    @Override
    public RowData nextRecord(RowData rowData) throws IOException {
        if (reachedEnd()) {
            return null;
        }
        // KafkaDynamicSource;
        // JsonFormatFactory;
        GenericRowData row = null;
        if (scanBuffer.size() > 0) {
            String field = scanBuffer.remove(0).getKey(); // result.get(0).getKey();
            String cacheKey = additionalKey + ":" + fieldNames[primaryKeyIndex] + ":" + field;
            if (cache != null) {
                row = cache.getIfPresent(cacheKey);
                if (row != null) {
                    return row;
                }
            }
            row = new GenericRowData(fieldNames.length);
            for (int i=0; i<fieldNum; i++) {
                String value = redisContainer.hget(additionalKey + ":" + fieldNames[i], field);
                row.setField(i, formatter.decode(value, logicalTypes[i]));
            }
            if (cache != null) {
                cache.put(cacheKey, row);
            }
        }
        // 如果缓冲区没有数据，尝试从redis拉取数据
        if (scanBuffer.size() <= 0  && !"0".equals(cursor)) {
            tryLoadDataFromRedis();
        }
        return row;
    }

    @Override
    public void close() throws IOException {
        //System.out.println("close");
        //if (redisContainer != null) {
        //    redisContainer.close();
        //}
    }
}
