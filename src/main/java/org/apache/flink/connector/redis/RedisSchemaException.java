package org.apache.flink.connector.redis;

/**
 * @author Liu Yang
 * @date 2023/1/17 8:17
 */
public class RedisSchemaException extends RuntimeException {

    public RedisSchemaException(String s) {
        super(s);
    }

    public RedisSchemaException(String s, Throwable t) {
        super(s, t);
    }

}
