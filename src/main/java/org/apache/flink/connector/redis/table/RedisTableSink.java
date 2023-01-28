package org.apache.flink.connector.redis.table;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.config.RedisSinkOptions;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * RedisTableSink
 *
 * @author Liu Yang
 * @date 2023/1/11 14:05
 */
public class RedisTableSink extends RichSinkFunction<RowData> {
    private final RedisConnectionOptions options;
    private final RedisMapper<RowData> mapper;
    private final RedisSinkOptions sinkOptions;
    private RedisContainer container;

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
            this.mapper.upsert(this.container, pk.toString(), value);
            this.writeWatermark(new Watermark(System.currentTimeMillis()));
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

}
