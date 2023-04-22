package org.apache.flink.connector.redis.mapper;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.redis.RedisUnsupportedException;
import org.apache.flink.connector.redis.formatter.RedisStoreStrategy;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import redis.clients.jedis.StreamEntryID;

import java.util.Locale;

/**
 * RedisMapperFactory
 *
 * @author Liu Yang
 * @date 2023/1/11 14:44
 */
public class RedisMapperFactory {

    public static Builder<RowData> builder(RedisStoreStrategy strategy) {
        switch (strategy) {
            case HASH:
                return new RedisHashMapperBuilder();
            case STREAM:
                return new RedisStreamMapperBuilder();
            case STRING:
                return new RedisStringMapperBuilder();
            case LIST:
            case SET:
            case ZSET:
            default:
                throw new RedisUnsupportedException("Unsupported redis store strategy " + strategy);
        }
    }

    public static Builder<RowData> builder(String strategyName) {
        RedisStoreStrategy strategy = RedisStoreStrategy.valueOf(strategyName.toUpperCase(Locale.ROOT));
        return builder(strategy);
    }

    public static abstract class Builder<T> {
        protected String format = "json";
        protected String additionalKey;
        protected Integer keyTtl;
        protected ObjectIdentifier identifier;
        protected DeserializationSchema<T> deserializationSchema;
        protected SerializationSchema<T> serializationSchema;
        protected String primaryKey;
        protected LogicalType resultType;
        protected String[] resultFieldNames;
        protected LogicalType[] resultFieldTypes;
        protected String groupName;
        protected String streamEntryId;

        private Builder() {

        }

        public Builder<T> setFormat(String format) {
            this.format = format;
            return this;
        }

        public Builder<T> setAdditionalKey(String additionalKey) {
            this.additionalKey = additionalKey;
            return this;
        }

        public Builder<T> setKeyTtl(Integer keyTtl) {
            this.keyTtl = keyTtl;
            return this;
        }

        public Builder<T> setIdentifier(ObjectIdentifier identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder<T> setPrimaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder<T> setResultType(LogicalType resultType) {
            this.resultType = resultType;
            return this;
        }

        public Builder<T> setResultFieldNames(String[] resultFieldNames) {
            this.resultFieldNames = resultFieldNames;
            return this;
        }

        public Builder<T> setResultFieldTypes(LogicalType[] resultFieldTypes) {
            this.resultFieldTypes = resultFieldTypes;
            return this;
        }

        public Builder<T> setDeserializationSchema(DeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            return this;
        }

        public Builder<T> setSerializationSchema(SerializationSchema<T> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return this;
        }

        public Builder<T> setGroupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder<T> setStreamEntryId(String streamEntryId) {
            this.streamEntryId = streamEntryId;
            return this;
        }

        // 填充数据
        protected void fill(RedisBaseMapper<T> mapper) {
            mapper.setAdditionalKey(additionalKey);
            mapper.setKeyTtl(keyTtl);
            mapper.setFormat(format);
            mapper.setIdentifier(identifier);
            mapper.setPrimaryKey(primaryKey);
            mapper.setResultType(resultType);
            mapper.setResultFieldNames(resultFieldNames);
            mapper.setResultFieldTypes(resultFieldTypes);
        }

        public abstract RedisMapper<T> build();
    }

    public static class RedisHashMapperBuilder extends Builder<RowData> {

        private RedisHashMapperBuilder () {
            super();
        }

        @Override
        public RedisMapper<RowData> build() {
            RedisHashMapper mapper = new RedisHashMapper();
            fill(mapper);
            return mapper;
        }
    }

    public static class RedisStreamMapperBuilder extends Builder<RowData> {

        private RedisStreamMapperBuilder () {
            super();
        }

        @Override
        public RedisMapper<RowData> build() {
            StreamEntryID entryId = streamEntryId == null ? null : new StreamEntryID(streamEntryId);
            RedisStreamMapper mapper = new RedisStreamMapper(groupName,entryId);
            fill(mapper);
            return mapper;
        }
    }

    public static class RedisStringMapperBuilder extends Builder<RowData> {

        private RedisStringMapperBuilder () {
            super();
        }

        @Override
        public RedisMapper<RowData> build() {
            RedisStringMapper mapper = new RedisStringMapper();
            fill(mapper);
            return mapper;
        }
    }
}
