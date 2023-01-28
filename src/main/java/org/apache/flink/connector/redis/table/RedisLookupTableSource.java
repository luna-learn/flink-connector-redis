package org.apache.flink.connector.redis.table;

import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.connector.redis.RedisSourceException;
import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.config.RedisLookupSourceOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * RedisLookupTableSource
 *
 * @author Liu Yang
 * @date 2023/1/11 9:39
 */
public class RedisLookupTableSource extends TableFunction<RowData> {

    private final RedisConnectionOptions options;
    private final RedisMapper<RowData> mapper;
    private final RedisLookupSourceOptions lookupOptions;

    private final String[] lookupKeys;

    private RedisContainer container;
    private Cache<String, GenericRowData> cache;

    public RedisLookupTableSource(RedisConnectionOptions options,
                                  RedisMapper<RowData> mapper,
                                  RedisLookupSourceOptions lookupOptions) {
        this.options = options;
        this.mapper = mapper;
        this.lookupOptions = lookupOptions;
        this.lookupKeys = lookupOptions.getLookupKeys();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        int cacheExpireMs = lookupOptions.getCacheExpireMs();
        int cacheMaxSize = lookupOptions.getCacheMaxSize();
        // 开启 redis container
        this.container = options.getContainer();
        this.container.open();
        // 尝试初始化缓存
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

    public GenericRowData lookup(String key) {
        // lookup cache
        GenericRowData row = restore(key);
        if (row != null) {
            return row;
        }
        // lookup redis
        Exception lastException = null;
        int retry = 0, maxRetryTimes = lookupOptions.getMaxRetryTimes();
        while(retry < maxRetryTimes) {
            try {
                row = (GenericRowData) mapper.query(container, key);
                // 缓存数据
                store(key, row);
                return row;
            } catch (IOException e) {
                lastException = e;
                retry++;
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
        throw new RedisSourceException("Execution of redis lookup failed.", lastException);
    }

    public void eval(Object... values) {
        if (values == null) {
            return;
        }
        if (values.length != lookupKeys.length) {
            throw new IllegalArgumentException("Input parameter number different from keys " +
                    " within redis source " + mapper.getIdentifier().asSummaryString() +
                    "[" + String.join(",", lookupKeys) + "], " +
                    "require " + lookupKeys.length + " values, but given " + values.length + ".");
        }
        // 查询数据
        String key = String.valueOf(values[0]);
        GenericRowData row = lookup(key);
        collect(row);
    }

    @Override
    public void close() throws Exception {
        if (container != null) {
            container.close();
        }
    }
}
