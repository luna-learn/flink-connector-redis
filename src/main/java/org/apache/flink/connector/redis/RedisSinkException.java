package org.apache.flink.connector.redis;

public class RedisSinkException extends RuntimeException {

    public RedisSinkException(String s) {
        super(s);
    }

    public RedisSinkException(String s, Throwable t) {
        super(s, t);
    }
}
