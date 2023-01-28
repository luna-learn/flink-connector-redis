package org.apache.flink.connector.redis.mapper;

import org.apache.flink.connector.redis.RedisUnsupportedException;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.formatter.RedisStoreStrategy;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Liu Yang
 * @date 2023/1/11 11:48
 */
public class RedisHashMapper extends RedisRowDataMapper {

    public RedisHashMapper() {

    }

    @Override
    protected void initialize() {
        super.initialize();

    }

    @Override
    public GenericRowData read(RedisContainer container, String key) throws IOException {
        if (key == null) {
            return null;
        }
        try {
            List<String> results = container.hmget(key, resultFieldNames);
            StringData pk = StringData.fromString(separateKey(key));
            RowData row = this.convertToRowData(results);
            return GenericRowData.of(pk, row);
        } catch (Exception e){
            throw new IOException("Query redis hash data error at " + key + ".", e);
        }

    }

    @Override
    public void write(RedisContainer container, String key, RowData row) throws IOException {
        if (key == null || row == null) {
            return ;
        }
        final int fieldNum = resultFieldNames.length;
        final Map<String, String> hash;
        final RowData rowData;
        if (resultType.getTypeRoot() == LogicalTypeRoot.ROW) {
            rowData = row.getRow(1, fieldNum);
            hash = this.convertToMap(rowData);
        } else {
            throw new RedisUnsupportedException("Not supported [" + key + "] non-row type row[1]: " + row);
        }
        try {
            // 写入数据
            container.hmset(key, hash);
            if (keyTtl != null && keyTtl > 0) {
                container.expire(key, keyTtl);
            }
        } catch (Exception e) {
            throw new IOException("Upsert redis hash data error , cause: " + e.getMessage(), e);
        }
    }

    @Override
    public RedisStoreStrategy getRedisStoreStrategy() {
        return RedisStoreStrategy.HASH;
    }
}
