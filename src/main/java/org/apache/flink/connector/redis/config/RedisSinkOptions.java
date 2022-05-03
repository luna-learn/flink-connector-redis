package org.apache.flink.connector.redis.config;

import java.io.Serializable;

public class RedisSinkOptions  implements Serializable {
    // sink - upsert
    private boolean useBuffer;
    private int bufferSize;
}
