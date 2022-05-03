package org.apache.flink.connector.redis.config;

import java.io.Serializable;

public class RedisSourceOptions implements Serializable {
    // source lookup
    private final int cacheMaxSize;
    private final int cacheExpireMs;
    private final int maxRetryTimes;
    private boolean isBounded = true;

    public RedisSourceOptions(int cacheMaxSize,
                              int cacheExpireMs,
                              int maxRetryTimes) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
    }

    public RedisSourceOptions(int cacheMaxSize,
                              int cacheExpireMs,
                              int maxRetryTimes,
                              boolean isBounded) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
        this.isBounded = isBounded;
    }

    public int getCacheMaxSize() {
        return cacheMaxSize;
    }

    public int getCacheExpireMs() {
        return cacheExpireMs;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public boolean isBounded() {
        return this.isBounded;
    }
}
