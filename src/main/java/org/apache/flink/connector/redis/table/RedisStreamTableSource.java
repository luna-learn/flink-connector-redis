package org.apache.flink.connector.redis.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.connector.redis.mapper.RedisStreamMapper;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

/**
 * RedisTableSource
 * @author Liu Yang
 * @date 2023/1/11 9:20
 */
public class RedisStreamTableSource extends RichSourceFunction<RowData> {

    private final RedisConnectionOptions options;
    private final RedisStreamMapper mapper;
    private final RedisSourceOptions sourceOptions;
    private RedisContainer container;

    public RedisStreamTableSource(RedisConnectionOptions options,
                                  RedisMapper<RowData> mapper,
                                  RedisSourceOptions sourceOptions) {
        this.options = options;
        this.mapper = (RedisStreamMapper) mapper;
        this.sourceOptions = sourceOptions;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        this.container = options.getContainer();
        this.container.open();
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        try {
            this.mapper.stream(this.container, row -> {
                ctx.collectWithTimestamp(row, System.currentTimeMillis());
            });
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }

    @Override
    public void close() throws Exception {
        if (this.container != null) {
            this.container.close();
        }
        if (this.mapper != null) {
            this.mapper.close();
        }
    }

    @Override
    public void cancel() {
        if (this.container != null) {
            this.container.close();
        }
        if (this.mapper != null) {
            this.mapper.close();
        }
    }
}
