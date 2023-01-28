package org.apache.flink.connector.redis.config;

import java.io.Serializable;

/**
 * RedisSourceOptions
 *
 * @author Liu Yang
 * @date 2023/1/11 9:16
 */
public class RedisLookupSourceOptions implements Serializable {

    private int cacheExpireMs = 60000;
    private int cacheMaxSize = 1000;
    private int maxRetryTimes = 3;
    private String[] lookupKeys;

    public RedisLookupSourceOptions() {

    }

    public int getCacheExpireMs() {
        return cacheExpireMs;
    }

    public int getCacheMaxSize() {
        return cacheMaxSize;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public String[] getLookupKeys() {
        return lookupKeys;
    }

    public void setLookupKeys(String[] lookupKeys) {
        this.lookupKeys = lookupKeys;
    }

    public void setCacheExpireMs(int cacheExpireMs) {
        this.cacheExpireMs = cacheExpireMs;
    }

    public void setCacheMaxSize(int cacheMaxSize) {
        this.cacheMaxSize = cacheMaxSize;
    }

    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }
}
