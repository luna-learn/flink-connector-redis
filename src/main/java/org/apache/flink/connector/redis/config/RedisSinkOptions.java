package org.apache.flink.connector.redis.config;

import lombok.Data;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;

/**
 * RedisSinkOptions*
 * @author Liu Yang
 * @date 2023/1/11 9:16
 */
@Data
public class RedisSinkOptions implements Serializable {

    private int ttl = -1;

    private int parallelism = 1;


    public static RedisSinkOptions from(ReadableConfig config) {
        RedisSinkOptions options = new RedisSinkOptions();
        options.ttl = config.getOptional(RedisOptions.KEY_TTL).orElse(-1);
        options.parallelism = config.getOptional(RedisOptions.SINK_PARALLELISM).orElse(1);
        return options;
    }


}
