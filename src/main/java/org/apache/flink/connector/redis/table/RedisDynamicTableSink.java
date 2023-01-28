package org.apache.flink.connector.redis.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.config.RedisSinkOptions;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/**
 * RedisDynamicTableSink
 *
 * @author Liu Yang
 * @date 2023/1/12 10:47
 */
public class RedisDynamicTableSink implements DynamicTableSink {
    private final RedisConnectionOptions options;
    private final ReadableConfig config;
    private final RedisMapper<RowData> mapper;

    public RedisDynamicTableSink(RedisConnectionOptions options,
                                 ReadableConfig config,
                                 RedisMapper<RowData> mapper) {
        this.options = options;
        this.config = config;
        this.mapper = mapper;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RedisSinkOptions sinkOptions = new RedisSinkOptions();
        return SinkFunctionProvider.of(new RedisTableSink(options, mapper, sinkOptions));
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(options, config, mapper);
    }

    @Override
    public String asSummaryString() {
        return "RedisDynamicTableSink";
    }
}
