package org.apache.flink.connector.redis.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.config.RedisOptions;
import org.apache.flink.connector.redis.mapper.RedisCommand;
import org.apache.flink.connector.redis.mapper.RedisMapper;
import org.apache.flink.connector.redis.mapper.RedisQueryMapper;
import org.apache.flink.connector.redis.mapper.RedisUpsertMapper;
import org.apache.flink.connector.redis.table.sink.RedisDynamicTableSink;
import org.apache.flink.connector.redis.table.source.RedisDynamicTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class RedisDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {


    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();
        // discover a suitable decoding format
        //final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
        //        SerializationFormatFactory.class,
        //        FactoryUtil.FORMAT);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        Optional<UniqueConstraint> uniqueConstraint = schema.getPrimaryKey();
        String primaryKeys = uniqueConstraint != null && uniqueConstraint.isPresent() ?
                uniqueConstraint.get().getColumns().get(0) : null;

        RedisMapper mapper = RedisUpsertMapper.builder()
                .setRedisCommand(RedisCommand.HSET)
                .setAdditionalKey(config.get(RedisOptions.ADDITIONAL_KEY))
                .setKeyTtl(config.get(RedisOptions.KEY_TTL))
                .setFieldNames(schema.getFieldNames())
                .setFieldTypes(schema.getFieldTypes())
                .setDataTypes(schema.getFieldDataTypes())
                .setPrimaryKey(primaryKeys)
                .build();
        return new RedisDynamicTableSink(RedisOptions.getConnectorOptions(config),
                RedisOptions.getSinkOptions(config),
                mapper, schema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();

        TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        Optional<UniqueConstraint> uniqueConstraint = schema.getPrimaryKey();
        String primaryKeys = uniqueConstraint != null && uniqueConstraint.isPresent() ?
                uniqueConstraint.get().getColumns().get(0) : null;

        RedisQueryMapper mapper = RedisQueryMapper.builder()
                .setRedisCommand(RedisCommand.HGET)
                .setAdditionalKey(config.get(RedisOptions.ADDITIONAL_KEY))
                .setFieldNames(schema.getFieldNames())
                .setFieldTypes(schema.getFieldTypes())
                .setDataTypes(schema.getFieldDataTypes())
                .setPrimaryKey(primaryKeys)
                .build();
        return new RedisDynamicTableSource(RedisOptions.getConnectorOptions(config),
                RedisOptions.getSourceOptions(config),
                mapper, schema);
    }

    @Override
    public String factoryIdentifier() {
        return "redis";
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

        return optional;
    }

    private void validate(ReadableConfig config) {

    }
}
