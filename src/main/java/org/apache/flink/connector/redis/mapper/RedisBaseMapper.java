package org.apache.flink.connector.redis.mapper;

import org.apache.flink.connector.redis.RedisUnsupportedException;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.formatter.RedisFormatter;
import org.apache.flink.connector.redis.formatter.RedisJsonFormatter;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * RedisBaseMapper
 *
 * @author Liu Yang
 * @date 2023/1/11 9:32
 */
public abstract class RedisBaseMapper<T> implements RedisMapper<T> {

    protected String           format = "json";
    protected String           additionalKey;
    protected Integer          keyTtl;
    protected ObjectIdentifier identifier;
    protected String           primaryKey;
    protected LogicalType      resultType;
    protected String[]         resultFieldNames;
    protected LogicalType[]    resultFieldTypes;

    protected List<RedisFormatter<Object, String>> formatters;
    protected RedisFormatter<Object, String> formatter;
    protected RowData.FieldGetter[] fieldGetters;

    public void close() {
        // TODO:
    }

    @Override
    public String getAdditionalKey() {
        return additionalKey;
    }

    @Override
    public String getFormat() {
        return format;
    }

    @Override
    public Integer getKeyTtl() {
        return keyTtl;
    }

    @Override
    public ObjectIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public String getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public LogicalType getResultType() {
        return resultType;
    }

    @Override
    public String[] getResultFieldNames() {
        return resultFieldNames;
    }

    @Override
    public LogicalType[] getResultFieldTypes() {
        return resultFieldTypes;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setAdditionalKey(String additionalKey) {
        this.additionalKey = additionalKey;
    }

    public void setKeyTtl(Integer keyTtl) {
        this.keyTtl = keyTtl;
    }

    public void setIdentifier(ObjectIdentifier identifier) {
        this.identifier = identifier;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public void setResultType(LogicalType resultType) {
        this.resultType = resultType;
    }

    public void setResultFieldNames(String[] resultFieldNames) {
        this.resultFieldNames = resultFieldNames;
    }

    public void setResultFieldTypes(LogicalType[] resultFieldTypes) {
        this.resultFieldTypes = resultFieldTypes;
    }

    protected void initialize() {
        // TODO: 初始化Mapper
        if (formatters == null) {
            formatters = new ArrayList<>();
            for(LogicalType logicalType : resultFieldTypes) {
                formatters.add(this.createFormatter(logicalType));
            }
        }
        if (fieldGetters == null) {
            int fieldNum = resultFieldTypes.length;
            fieldGetters = new RowData.FieldGetter[resultFieldTypes.length];
            for (int i = 0; i < fieldNum; i++) {
                fieldGetters[i] = RowData.createFieldGetter(resultFieldTypes[i], i);
            }
        }
    }

    protected RedisFormatter<Object, String> createFormatter(LogicalType logicalType) {
        switch(format) {
            case "json":
                return new RedisJsonFormatter(logicalType);
            case "csv":
            case "raw":
            default:
                throw new RedisUnsupportedException("Not supported type[" + logicalType+ "] format[" + format + "]");
        }
    }

    // 生成 key
    protected String generateKey(String key) {
        if (additionalKey == null || key == null) {
            return key;
        } else {
            return additionalKey + ":" + key;
        }
    }

    // 拆分 key
    protected String separateKey(String key) {
        if (additionalKey == null || key == null) {
            return key;
        } else {
            return key.replace(additionalKey + ":", "");
        }
    }

    protected abstract T read(RedisContainer container, String key) throws IOException;

    protected abstract void write(RedisContainer container, String key, T row) throws IOException;

    @Override
    public T query(RedisContainer container, String key) throws IOException {
        initialize();
        String hashKey = generateKey(key);
        return read(container, hashKey);
    }

    @Override
    public void upsert(RedisContainer container, String key, T value) throws IOException {
        initialize();
        String hashKey = generateKey(key);
        write(container, hashKey, value);
    }

    @Override
    public void scan(RedisContainer container, ScanParams params, Consumer<T> consumer) throws IOException {
        initialize();
        boolean running = true;
        ScanResult<String> scanResult;
        List<String> scanBuffer;
        String cursor = ScanParams.SCAN_POINTER_START;
        int retry = 0, max = 3;
        // 设置 scan 参数
        String keyPattern = generateKey("*");
        params.match(keyPattern);
        params.count(100);
        // 尝试扫描 key 并读取数据
        while(running && !container.isClosed() && retry < max) {
            try {
                scanResult = container.scan(cursor, params);
                scanBuffer = scanResult.getResult();
                if (scanBuffer != null) {
                    for(String key: scanBuffer) {
                        T row = read(container, key);
                        if (consumer != null) {
                            consumer.accept(row);
                        }
                    }
                }
                if (scanResult.isCompleteIteration() ) {
                    running = false;
                } else {
                    cursor = scanResult.getCursor(); // 记录扫描游标
                }
            } catch (Exception e) {
                retry++;
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stream(RedisContainer container, Consumer<T> consumer) throws IOException {
        // TODO: 默认不支持 stream 方法
        throw new RedisUnsupportedException("Unsupported stream operation.");
    }

}
