package org.apache.flink.connector.redis.mapper;

import org.apache.flink.connector.redis.RedisUnsupportedException;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.formatter.RedisFormatterUtils;
import org.apache.flink.connector.redis.formatter.RedisStoreStrategy;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.IOException;

/**
 * @author Liu Yang
 * @date 2023/1/12 11:08
 */
public class RedisStringMapper extends RedisRowDataMapper {

    public RedisStringMapper() {

    }

    @Override
    public GenericRowData read(RedisContainer container, String key) throws IOException {
        if (key == null) {
            return null;
        }
        String value;
        try {
            value = container.get(key);
            Object fieldValue = RedisFormatterUtils.isNull(value) ?
                    null : formatters.get(0).deserialize(value);
            return GenericRowData.of(StringData.fromString(separateKey(key)),
                    fieldValue);
        } catch (Exception e) {
            throw new IOException("Query redis string data error at " + key + ".", e);
        }
    }

    @Override
    public void write(RedisContainer container, String key, RowData row) throws IOException {
        if (key == null || row == null) {
            return ;
        }
        Object fieldValue;
        try {
            // 写入数据
            if (resultType.getTypeRoot() == LogicalTypeRoot.ROW) {
                final int fieldNum = resultFieldNames.length;
                fieldValue = row.getRow(1, fieldNum);
            } else if (resultType.getTypeRoot() == LogicalTypeRoot.VARCHAR) {
                fieldValue = row.getString(1);
            } else {
                throw new RedisUnsupportedException("Not supported [" + key + "] " + resultType + " type row[1]: " + row);
            }
            String value = formatters.get(0).serialize(fieldValue);
            container.set(key, value);
            if (keyTtl != null && keyTtl > 0) {
                container.expire(key, keyTtl);
            }
        } catch (Exception e){
            throw new IOException("Upsert string hash data error at [" + row + "], cause: " + e.getMessage(), e);
        }
    }

    @Override
    public RedisStoreStrategy getRedisStoreStrategy() {
        return RedisStoreStrategy.STRING;
    }
}
