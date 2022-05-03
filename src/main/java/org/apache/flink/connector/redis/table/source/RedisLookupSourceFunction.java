package org.apache.flink.connector.redis.table.source;


import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisLookupSourceFunction implements SourceFunction<Row> {
    private final RedisConnectorOptions connectorOptions;
    private final RedisSourceOptions sourceOptions;
    private final RedisMapper redisMapper;
    private RedisContainer redisContainer;

    private Logger LOG = LoggerFactory.getLogger(RedisLookupSourceFunction.class);

    public RedisLookupSourceFunction(RedisConnectorOptions connectorOptions,
                                     RedisSourceOptions sourceOptions,
                                     RedisMapper redisMapper) {
        this.connectorOptions = connectorOptions;
        this.sourceOptions = sourceOptions;
        this.redisMapper = redisMapper;
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {


    }

    @Override
    public void cancel() {

    }
}
