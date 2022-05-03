package org.apache.flink.connector.redis.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.redis.UnsupportedRedisException;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;

public class RedisQueryMapper extends RedisBaseMapper {
    private static final RedisCommand[] OPTIONAL_COMMAND = {
            RedisCommand.GET,
            RedisCommand.HGET};

    private String[] queryKeys;

    private RedisQueryMapper() {

    }

    public void setQueryKeys(String[] queryKeys) {
        this.queryKeys = queryKeys;
    }

    public String[] getQueryKeys() {
        return queryKeys;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        RedisQueryMapper mapper;
        private Builder() {
            mapper = new RedisQueryMapper();
            mapper.redisCommand = RedisCommand.HGET;
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

        public Builder setPrimaryKey(String primaryKey) {
            mapper.primaryKey = primaryKey;
            return this;
        }

        public Builder setQueryKeys(String[] queryKeys) {
            mapper.queryKeys = queryKeys;
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

        public RedisQueryMapper build() {
            return mapper;
        }

    }
}
