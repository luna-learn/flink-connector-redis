package org.apache.flink.connector.redis;

public class UnsupportedRedisException extends RuntimeException {

    public UnsupportedRedisException(String s) {
        super(s);
    }

    public UnsupportedRedisException(String s, Throwable t) {
        super(s, t);
    }
}
