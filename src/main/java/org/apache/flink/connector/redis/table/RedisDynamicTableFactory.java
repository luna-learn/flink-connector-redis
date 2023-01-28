package org.apache.flink.connector.redis.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.RedisSchemaException;
import org.apache.flink.connector.redis.RedisSourceException;
import org.apache.flink.connector.redis.RedisUnsupportedException;
import org.apache.flink.connector.redis.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.config.RedisOptions;
import org.apache.flink.connector.redis.formatter.RedisStoreStrategy;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.connector.redis.mapper.RedisMapperFactory;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * RedisDynamicTableFactory
 *
 * @author Liu Yang
 * @date 2023/1/11 16:04
 */
public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String identifier = "redis";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();

        // 尝试查找相关的 DeserializationFormatFactory
        if (config.get(FactoryUtil.FORMAT) != null) {
            helper.discoverDecodingFormat(
                    DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        }

        RedisConnectionOptions options = RedisConnectionOptions.getConnectorOptions(config);

        RedisMapper<RowData> mapper = createRedisMapper(context, config);

        return new RedisDynamicTableSink(options, config, mapper);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();

        // 尝试查找相关的 SerializationFormatFactory
        if (config.get(FactoryUtil.FORMAT) != null) {
            helper.discoverEncodingFormat(
                    SerializationFormatFactory.class, FactoryUtil.FORMAT);
        }

        RedisConnectionOptions options = RedisConnectionOptions.getConnectorOptions(config);

        final TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        RedisMapper<RowData> mapper = createRedisMapper(context, config);

        return new RedisDynamicTableSource(options, config, mapper, schema);
    }

    @Override
    public String factoryIdentifier() {
        return identifier;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> required = new HashSet<>();
        required.add(FactoryUtil.CONNECTOR);
        required.add(RedisOptions.MODE);
        return required;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optional = new HashSet<>();
        optional.add(FactoryUtil.FORMAT);
        
        optional.add(RedisOptions.TYPE);

        optional.add(RedisOptions.PASSWORD);
        optional.add(RedisOptions.MAX_TOTAL);
        optional.add(RedisOptions.MAX_IDLE);
        optional.add(RedisOptions.MIN_IDLE);
        optional.add(RedisOptions.DATABASE);
        optional.add(RedisOptions.TIMEOUT);

        optional.add(RedisOptions.SO_TIMEOUT);

        optional.add(RedisOptions.HOST);
        optional.add(RedisOptions.PORT);

        optional.add(RedisOptions.SENTINEL_MASTER);
        optional.add(RedisOptions.SENTINEL_NODES);

        optional.add(RedisOptions.CLUSTER_NODES);
        optional.add(RedisOptions.CLUSTER_MAX_REDIRECTIONS);

        optional.add(RedisOptions.ADDITIONAL_KEY);
        optional.add(RedisOptions.KEY_TTL);

        optional.add(RedisOptions.LOOKUP_CACHE_MAX_SIZE);
        optional.add(RedisOptions.LOOKUP_CACHE_EXPIRE_MS);
        optional.add(RedisOptions.LOOKUP_MAX_RETRY_TIMES);
        optional.add(RedisOptions.SCAN_IS_BOUNDED);

        optional.add(RedisOptions.STEAM_GROUP_NAME);
        optional.add(RedisOptions.STEAM_ENTRY_ID);
        return optional;
    }

    private void verifyTableSchema(Context context) {
        final ObjectIdentifier identifier = context.getObjectIdentifier();
        final TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        final Optional<UniqueConstraint> uniqueConstraint = schema.getPrimaryKey();
        // 验证字段数量：有且只能有两个字段
        if (schema.getFieldCount() > 2) {
            throw new RedisSchemaException("Redis table " + identifier.asSummaryString()
                    + " only support 2 columns, " +
                    "but provided " + schema.getFieldCount() + " columns");
        }
        // 验证主键：只能有一个，且必须是第一个字段且类型为 VARCHAR
        if (uniqueConstraint.isPresent()) {
            // 主键只能有一个
            List<String> primaryKeys = uniqueConstraint.get().getColumns();
            if (primaryKeys.size() != 1) {
                throw new RedisSourceException("Redis table " + identifier.asSummaryString()
                        + " only support one primary key, " +
                        "but provided [" + String.join(",", primaryKeys) + "]" +
                        " (" + primaryKeys.size() + " keys)");
            }
            Optional<TableColumn> firstKey = schema.getTableColumn(0);
            Optional<TableColumn> primaryKey = schema.getTableColumn(primaryKeys.get(0));
            if (primaryKey.isPresent()) {
                // 主键必须为字符类型
                LogicalType logicalType = primaryKey.get().getType().getLogicalType();
                if (!LogicalTypeChecks.hasFamily(logicalType, LogicalTypeFamily.CHARACTER_STRING)) {
                    throw new RedisSchemaException("Redis table " + identifier.asSummaryString()
                            + " primary key type unsupported non-string type, " +
                            "but provided [" + logicalType + "].");
                }
                // 主键必须是第一个键
                if (firstKey.isPresent() && !firstKey.get().equals(primaryKey.get())){
                    throw new RedisSchemaException("Redis table " + identifier.asSummaryString()
                            + " primary key must be set to first key like [" + primaryKey.get() + "], " +
                            "but provided first key is [" + firstKey.get() + "].");
                }
            } else {
                String columns = schema.getTableColumns().stream().map(TableColumn::getName)
                        .collect(Collectors.joining(","));
                throw new RedisSchemaException("Redis table " + identifier.asSummaryString()
                        + " primary key must in columns [" + columns + "]");
            }
        } else {
            throw new RedisSchemaException("Redis table " + identifier.asSummaryString()
                    + " required 1 primary key, but provided [] (0 keys).");
        }

    }

    private RedisMapper<RowData> createRedisMapper(Context context, ReadableConfig config) {
        // 验证表结构
        verifyTableSchema(context);
        // 取出第一个、第二个字段进行处理
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        ObjectIdentifier identifier = context.getObjectIdentifier();
        Optional<TableColumn> firstKeyOptional = schema.getTableColumn(0);
        Optional<TableColumn> secondKeyOptional = schema.getTableColumn(1);
        if (!firstKeyOptional.isPresent() || !secondKeyOptional.isPresent()) {
            throw new IllegalArgumentException();
        }
        final TableColumn firstKey = firstKeyOptional.get();
        final TableColumn secondKey = secondKeyOptional.get();
        final String primaryKey = firstKey.getName();
        final DataType resultType = secondKey.getType();
        final List<String> resultFieldNames = new ArrayList<>();
        final List<LogicalType> resultFieldTypes = new ArrayList<>();
        // Hash 策略
        Consumer<LogicalType> hashStrategyHandler = (logicalType) -> {
            // HASH 策略的第二字段必须是 ROW 类型
            if (logicalType.getTypeRoot() == LogicalTypeRoot.ROW) {
                RowType rowType = (RowType) logicalType;
                resultFieldNames.addAll(rowType.getFieldNames());
                resultFieldTypes.addAll(rowType.getChildren());
            } else {
                throw new RedisUnsupportedException("Redis hash store strategy second key not support type [" + resultType + "].");
            }
        };
        // String 策略
        Consumer<LogicalType> stringStrategyConsumer = (logicalType) -> {
            resultFieldNames.add(secondKey.getName());
            resultFieldTypes.add(secondKey.getType().getLogicalType());
        };
        // 处理 strategy
        RedisStoreStrategy strategy = RedisStoreStrategy.valueOf(config.get(RedisOptions.TYPE).toUpperCase(Locale.ROOT));
        switch(strategy) {
            case HASH:
            case STREAM:
                hashStrategyHandler.accept(resultType.getLogicalType());
                break;
            case STRING:
                stringStrategyConsumer.accept(resultType.getLogicalType());
                break;
            default:
                throw new RedisUnsupportedException("Unsupported redis store strategy " + strategy + ".");
        }
        // 生成 redis mapper
        final RedisMapper<RowData> mapper = RedisMapperFactory.builder(config.get(RedisOptions.TYPE))
                .setAdditionalKey(config.get(RedisOptions.ADDITIONAL_KEY))
                .setIdentifier(identifier)
                .setKeyTtl(config.get(RedisOptions.KEY_TTL))
                .setPrimaryKey(primaryKey)
                .setResultType(resultType.getLogicalType())
                .setResultFieldNames(resultFieldNames.toArray(new String[0]))
                .setResultFieldTypes(resultFieldTypes.toArray(new LogicalType[0]))
                .setGroupName(config.get(RedisOptions.STEAM_GROUP_NAME))
                .setStreamEntryId(config.get(RedisOptions.STEAM_ENTRY_ID))
                .build();
        return mapper;
    }
}
