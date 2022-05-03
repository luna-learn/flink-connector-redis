package org.apache.flink.connector.redis.table.source;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.mapper.RedisQueryMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;

public class RedisLookupableTableSource implements LookupableTableSource<RowData> {

    private final RedisConnectorOptions connectorOptions;
    private final RedisSourceOptions sourceOptions;
    private final RedisQueryMapper redisMapper;

    public RedisLookupableTableSource(RedisConnectorOptions connectorOptions,
                                      RedisSourceOptions sourceOptions,
                                      RedisQueryMapper redisMapper) {
        this.connectorOptions = connectorOptions;
        this.sourceOptions = sourceOptions;
        this.redisMapper = redisMapper;
    }

    @Override
    public TableFunction<RowData> getLookupFunction(String[] strings) {
        return new RedisLookupTableFunction(connectorOptions,
                sourceOptions,
                redisMapper);
    }

    @Override
    public AsyncTableFunction<RowData> getAsyncLookupFunction(String[] strings) {
        return null;
    }

    @Override
    public boolean isAsyncEnabled() {
        return false;
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .fields(redisMapper.getFieldNames(),
                        TypeConversions.fromLegacyInfoToDataType(redisMapper.getFieldTypes()))
                .build();
    }

    @Override
    public DataType getProducedDataType() {
        // 旧版本的Typeinfo类型转新版本的DataType
        return TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(redisMapper.getFieldTypes(),
                redisMapper.getFieldNames()));
    }
}
