package org.apache.flink.connector.redis.mapper;

import org.apache.flink.connector.redis.formatter.RedisFormatterUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * RedisBaseMapper
 *
 * @author Liu Yang
 * @date 2023/1/11 9:32
 */
public abstract class RedisRowDataMapper extends RedisBaseMapper<RowData> {

    protected RowData convertToRowData(Map<String, String> values) throws IOException {
        int i = 0, fieldNum = resultFieldNames.length;
        String fieldName = null, fieldValue = null;
        Object[] fieldValues = new Object[fieldNum];
        try {
            for(; i < fieldNum; i++) {
                fieldName = resultFieldNames[i];
                fieldValue = values.get(fieldName);
                if (RedisFormatterUtils.isNull(fieldValue)) {
                    fieldValues[i] = null;
                } else {
                    fieldValues[i] = formatters.get(i).deserialize(fieldValue);
                }
            }
        } catch (Exception e){
            throw new IOException("Redis deserialize value error at" +
                    " fieldName = " + fieldName +
                    ", fieldValue = " + fieldValue + "" +
                    ", fieldType = " + resultFieldTypes[i] + ".", e);
        }
        return GenericRowData.of(fieldValues);
    }

    protected RowData convertToRowData(List<String> values) throws IOException {
        int i = 0, fieldNum = resultFieldNames.length;
        String fieldName = null, fieldValue = null;
        Object[] fieldValues = new Object[fieldNum];
        try {
            for(; i < fieldNum; i++) {
                fieldName = resultFieldNames[i];
                fieldValue = values.get(i);
                if (RedisFormatterUtils.isNull(fieldValue)) {
                    fieldValues[i] = null;
                } else {
                    fieldValues[i] = formatters.get(i).deserialize(fieldValue);
                }
            }
        } catch (Exception e){
            throw new IOException("Redis deserialize value error at [" + i + "]" +
                    ", fieldName = " + fieldName +
                    ", fieldValue = " + fieldValue + ".", e);
        }
        return GenericRowData.of(fieldValues);
    }

    protected Map<String, String> convertToMap(RowData rowData) throws IOException {
        if (rowData == null) {
            return null;
        }
        int fieldNum = resultFieldNames.length;
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < fieldNum; i++) {
            try {
                Object fieldValue = fieldGetters[i].getFieldOrNull(rowData);
                if (fieldValue == null) {
                    map.put(resultFieldNames[i], "null");
                } else {
                    map.put(resultFieldNames[i], formatters.get(i).serialize(fieldValue));
                }
            } catch (Exception e) {
                throw new IOException("Redis serialize value error at " + resultFieldNames[i] + ", cause: " + e.getMessage(), e);
            }
        }
        return map;
    }
}
