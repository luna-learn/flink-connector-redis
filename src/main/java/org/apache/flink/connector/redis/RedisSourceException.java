package org.apache.flink.connector.redis;

public class RedisSourceException extends RuntimeException {

    public RedisSourceException(String s) {
        super(s);
    }

    public RedisSourceException(String s, Throwable t) {
        super(s, t);
    }
}
