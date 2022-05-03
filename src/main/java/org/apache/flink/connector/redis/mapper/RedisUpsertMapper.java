package org.apache.flink.connector.redis.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.redis.UnsupportedRedisException;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;

public class RedisUpsertMapper extends RedisBaseMapper {
    private static final RedisCommand[] OPTIONAL_COMMAND = {
            RedisCommand.SET,
            RedisCommand.HSET};

    private RedisUpsertMapper() {

    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        RedisUpsertMapper mapper;
        private Builder() {
            mapper = new RedisUpsertMapper();
            mapper.redisCommand = RedisCommand.HSET;
        }

        public Builder setRedisCommand(RedisCommand redisCommand) {
            if (Arrays.asList(OPTIONAL_COMMAND).contains(redisCommand)) {
                mapper.redisCommand = redisCommand;
            } else {
                throw new UnsupportedRedisException("RedisMapper unsupported" +
                        " redis command " + redisCommand);
            }
            return this;
        }

        public Builder setAdditionalKey(String additionalKey) {
            mapper.additionalKey = additionalKey;
            return this;
        }

        public Builder setKeyTtl(Integer ttl) {
            mapper.ttl = ttl;
            return this;
        }

        public Builder setPrimaryKey(String primaryKey) {
            mapper.primaryKey = primaryKey;
            return this;
        }

        public Builder setFieldNames(String[] fieldNames) {
            mapper.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(TypeInformation<?>[] fieldTypes) {
            mapper.fieldTypes = fieldTypes;
            return this;
        }

        public Builder setDataTypes(DataType[] dataTypes) {
            mapper.dataTypes = dataTypes;
            return this;
        }

        public RedisUpsertMapper build() {
            return mapper;
        }

    }
}
