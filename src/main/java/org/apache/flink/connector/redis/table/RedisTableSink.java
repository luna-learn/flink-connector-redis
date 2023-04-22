package org.apache.flink.connector.redis.table;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.config.RedisSinkOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * RedisTableSink
 *
 * @author Liu Yang
 * @date 2023/1/11 14:05
 */
public class RedisTableSink extends RichSinkFunction<RowData> implements CheckpointedFunction {
    private final RedisConnectionOptions options;
    private final RedisMapper<RowData> mapper;
    private final RedisSinkOptions sinkOptions;
    private RedisContainer container;
    private OperatorIOMetricGroup ioMetric;
    private long writeRecords = 0;

    private ListState<Long> writeRecordState;

    public RedisTableSink(RedisConnectionOptions options,
                          RedisMapper<RowData> mapper,
                          RedisSinkOptions sinkOptions) {
        this.options = options;
        this.mapper = mapper;
        this.sinkOptions = sinkOptions;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.container = options.getContainer();
        this.container.open();
        ioMetric =  getRuntimeContext().getMetricGroup().getIOMetricGroup();
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        if (value == null) {
            return ;
        }
        StringData pk = value.getString(0);
        if (pk == null) {
            return ;
        }
        try {
            mapper.upsert(this.container, pk.toString(), value);
            ioMetric.getNumRecordsOutCounter().inc();
            writeRecords++;
            writeWatermark(new Watermark(System.currentTimeMillis()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        if (this.container != null) {
            this.container.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        writeRecordState.clear();
        writeRecordState.add(writeRecords);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        writeRecordState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("WriteRecords", Long.class));
        if (context.isRestored()) {
            for(Long value: writeRecordState.get()) {
                writeRecords = value;
            }
        }
    }
}
