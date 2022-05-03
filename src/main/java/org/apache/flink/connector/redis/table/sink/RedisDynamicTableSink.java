package org.apache.flink.connector.redis.table.sink;

import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisSinkOptions;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

import static org.apache.flink.util.Preconditions.checkState;

public class RedisDynamicTableSink implements DynamicTableSink {
    private final RedisConnectorOptions connectorOptions;
    private final RedisSinkOptions sinkOptions;
    private final RedisMapper redisMapper;
    private final TableSchema schema;

    public RedisDynamicTableSink(RedisConnectorOptions connectorOptions,
                                 RedisSinkOptions sinkOptions,
                                 RedisMapper redisMapper,
                                 TableSchema schema) {
        this.connectorOptions = connectorOptions;
        this.sinkOptions = sinkOptions;
        this.redisMapper = redisMapper;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        // validatePrimaryKey(changelogMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(new RedisSink(connectorOptions, sinkOptions, redisMapper));
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(connectorOptions, sinkOptions, redisMapper, schema);
    }

    @Override
    public String asSummaryString() {
        return "RedisDynamicTableSink";
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(ChangelogMode.insertOnly().equals(requestedMode),
                "please declare primary key for sink table when query contains update/delete record.");
    }
}
