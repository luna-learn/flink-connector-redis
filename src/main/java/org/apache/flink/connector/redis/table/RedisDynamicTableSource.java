package org.apache.flink.connector.redis.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.RedisUnsupportedException;
import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.config.RedisLookupSourceOptions;
import org.apache.flink.connector.redis.config.RedisSourceOptions;
import org.apache.flink.connector.redis.formatter.RedisStoreStrategy;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Objects;

/**
 * RedisDynamicTable
 *
 * @author Liu Yang
 * @date 2023/1/11 15:32
 */
public class RedisDynamicTableSource implements LookupTableSource, ScanTableSource {
    private final RedisConnectionOptions options;
    private final ReadableConfig config;
    private final RedisMapper<RowData> mapper;
    private final TableSchema schema;

    public RedisDynamicTableSource(RedisConnectionOptions connectorOptions,
                                   ReadableConfig config,
                                   RedisMapper<RowData> mapper,
                                   TableSchema schema) {
        this.options = connectorOptions;
        this.config = config;
        this.mapper = mapper;
        this.schema = schema;
    }


    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // 验证 lookup 指定的 key
        String[] keyNames = verifyLookupPrimaryKey(context);

        final RedisLookupSourceOptions lookupOptions = new RedisLookupSourceOptions();
        lookupOptions.setLookupKeys(keyNames);
        
        return TableFunctionProvider.of(new RedisLookupTableSource(options, mapper, lookupOptions));
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        RedisSourceOptions sourceOptions = new RedisSourceOptions();
        RedisStoreStrategy strategy = mapper.getRedisStoreStrategy();
        switch(strategy) {
            case STRING:
            case HASH:
                return SourceFunctionProvider.of(new RedisScanTableSource(options, mapper, sourceOptions), true);
            case STREAM:
                return SourceFunctionProvider.of(new RedisStreamTableSource(options, mapper, sourceOptions), false);
            default:
                throw new RedisUnsupportedException("Unsupported redis store strategy " + strategy);
        }

    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(options, config, mapper, schema);
    }

    @Override
    public String asSummaryString() {
        return "RedisDynamicTableSource";
    }

    // 验证 lookup key
    private String[] verifyLookupPrimaryKey(LookupContext context) {
        String[] fieldNames = schema.getFieldNames();
        String[] keyNames = new String[context.getKeys().length];
        // 取出 lookup 指定的 key
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1,
                    "Redis source " + mapper.getIdentifier().asSummaryString() +
                            " only support non-nested look up keys," +
                            " but provided " + innerKeyArr.length + " keys.");
            keyNames[i] = fieldNames[innerKeyArr[0]];
        }
        // 检查是否包含 primary key 值
        boolean containsPrimaryKey = Arrays.stream(keyNames)
                .anyMatch(e -> Objects.equals(e, mapper.getPrimaryKey()));
        Preconditions.checkArgument(
                containsPrimaryKey,
                "Redis source " + mapper.getIdentifier().asSummaryString() +
                        " lookup keys must contains primary key [" + mapper.getPrimaryKey() + "], " +
                        "but provided [" + String.join(",", keyNames) + "].");
        // 检查是否查询的 key 大于 1 个
        Preconditions.checkArgument(
                keyNames.length == 1,
                "Redis source " + mapper.getIdentifier().asSummaryString() +
                        " only support one look up primary key [" + mapper.getPrimaryKey() + "]," +
                        " but provided [" + String.join(",", keyNames) + "].");
        return keyNames;
    }
}
