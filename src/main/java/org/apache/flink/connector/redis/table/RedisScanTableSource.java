package org.apache.flink.connector.redis.table;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import redis.clients.jedis.params.ScanParams;

/**
 * RedisTableSource
 * @author Liu Yang
 * @date 2023/1/11 9:20
 */
public class RedisScanTableSource extends RichSourceFunction<RowData> implements CheckpointedFunction {

    private final RedisConnectionOptions options;
    private final RedisMapper<RowData> mapper;
    private final RedisSourceOptions sourceOptions;
    private RedisContainer container;
    private OperatorIOMetricGroup ioMetric;
    private long readRecords = 0;
    private ListState<Long> readRecordState;

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
        this.ioMetric = getRuntimeContext().getMetricGroup().getIOMetricGroup();
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        ScanParams params = new ScanParams().count(10);
        this.mapper.scan(this.container, params, row -> {
            ioMetric.getNumRecordsInCounter().inc();
            ctx.collectWithTimestamp(row, System.currentTimeMillis());
            ioMetric.getNumRecordsOutCounter().inc();
            readRecords++;
            ctx.markAsTemporarilyIdle();
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

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        readRecordState.clear();
        readRecordState.add(readRecords);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        readRecordState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("ReadRecords", Long.class));
        if (context.isRestored()) {
            for(Long value: readRecordState.get()) {
                readRecords = value;
            }
        }
    }
}
