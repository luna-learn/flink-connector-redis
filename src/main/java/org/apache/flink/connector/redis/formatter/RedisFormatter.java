package org.apache.flink.connector.redis.formatter;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;

/**
 * RedisFormatter
 *
 * @author Liu Yang
 * @date 2023/1/16 9:11
 */
public interface RedisFormatter<T, U> extends Serializable {

    default DeserializationSchema<T> getDeserializationSchema() {
        return null;
    }


    default SerializationSchema<T> getSerializationSchema() {
        return null;
    }

    T deserialize(U message) throws IOException;

    default void deserialize(U message, Collector<T> out) throws IOException {
        T deserialize = deserialize(message);
        if (deserialize != null) {
            out.collect(deserialize);
        }
    }

    U serialize(T value) throws IOException;


}
