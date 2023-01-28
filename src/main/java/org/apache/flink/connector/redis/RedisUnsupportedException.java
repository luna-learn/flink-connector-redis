package org.apache.flink.connector.redis;

public class RedisUnsupportedException extends RuntimeException {

    public RedisUnsupportedException(String s) {
        super(s);
    }

    public RedisUnsupportedException(String s, Throwable t) {
        super(s, t);
    }
}
