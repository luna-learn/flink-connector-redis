package org.apache.flink.connector.redis.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.Set;

public interface RedisMapper extends Serializable {

    RedisCommand getCommand();

    Integer getKeyTtl();

    String getAdditionalKey();

    String getPrimaryKey();

    String[] getFieldNames();

    TypeInformation<?>[] getFieldTypes();

    DataType[] getDataTypes();

    Set<RedisCommand> getOptionalCommand();

}
