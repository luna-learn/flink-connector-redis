package org.apache.flink.connector.redis.table.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.redis.config.RedisConnectorOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.mapper.RedisQueryMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

public class RedisDynamicTableSource implements LookupTableSource, ScanTableSource {


    private final RedisConnectorOptions connectorOptions;
    private final RedisSourceOptions sourceOptions;
    private final RedisQueryMapper redisMapper;
    private final TableSchema schema;
    private final DataType physicalDataType;

    public RedisDynamicTableSource(RedisConnectorOptions connectorOptions,
                                   RedisSourceOptions sourceOptions,
                                   RedisQueryMapper redisMapper,
                                   TableSchema schema) {
        this.connectorOptions = connectorOptions;
        this.sourceOptions = sourceOptions;
        this.redisMapper = redisMapper;
        this.schema = schema;
        this.physicalDataType = schema.toPhysicalRowDataType();
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(connectorOptions,
                sourceOptions,
                redisMapper, schema);
    }

    @Override
    public String asSummaryString() {
        return "RedisDynamicTableSource";
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        String[] keyNames = new String[lookupContext.getKeys().length];

        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = lookupContext.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "Redis only support non-nested look up keys");
            keyNames[i] = schema.getFieldNames()[innerKeyArr[0]];
        }
        redisMapper.setQueryKeys(keyNames);
        boolean containsPrimaryKey = Arrays.stream(keyNames)
                .anyMatch(e -> Objects.equals(e, redisMapper.getPrimaryKey()));
        // 检查是否包含主键
        Preconditions.checkArgument(
                containsPrimaryKey,
                "Redis lookup keys must contains primary key [" + redisMapper.getPrimaryKey() + "], but provided [" +
                String.join(",", keyNames) + "]");
        return TableFunctionProvider.of(new RedisLookupTableFunction(connectorOptions,
                sourceOptions,
                redisMapper));
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // return ChangelogMode.all();
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        if (sourceOptions.isBounded()) {
            return InputFormatProvider.of(new RedisScanTableSource(connectorOptions,
                    sourceOptions,
                    redisMapper));
        } else {
            return SourceFunctionProvider.of(new RedisUnboundTableSource(connectorOptions,
                    sourceOptions,
                    redisMapper), sourceOptions.isBounded());
        }
    }

    private DeserializationSchema<RowData> createDeserialization(
            Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        } else {
            DataType physicalFormatDataType = DataTypeUtils.projectRow(this.physicalDataType, projection);
            if (prefix != null) {
                physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
            }

            return (DeserializationSchema)format.createRuntimeDecoder(context, physicalFormatDataType);
        }
    }
}
