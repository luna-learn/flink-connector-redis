package org.apache.flink.connector.redis.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import redis.clients.jedis.params.ScanParams;

/**
 * RedisTableSource
 * @author Liu Yang
 * @date 2023/1/11 9:20
 */
public class RedisScanTableSource extends RichSourceFunction<RowData> {

    private final RedisConnectionOptions options;
    private final RedisMapper<RowData> mapper;
    private final RedisSourceOptions sourceOptions;
    private RedisContainer container;

    public RedisScanTableSource(RedisConnectionOptions options,
                                RedisMapper<RowData> mapper,
                                RedisSourceOptions sourceOptions) {
        this.options = options;
        this.mapper = mapper;
        this.sourceOptions = sourceOptions;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        this.container = options.getContainer();
        this.container.open();
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        ScanParams params = new ScanParams().count(10);
        this.mapper.scan(this.container, params, row -> {
            ctx.collectWithTimestamp(row, System.currentTimeMillis());
        });
    }

    @Override
    public void close() throws Exception {
        if (this.container != null) {
            this.container.close();
        }
    }

    @Override
    public void cancel() {
        if (this.container != null) {
            this.container.close();
        }
    }
}
