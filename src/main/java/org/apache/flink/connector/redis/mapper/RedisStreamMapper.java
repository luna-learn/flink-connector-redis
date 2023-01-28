package org.apache.flink.connector.redis.mapper;

import org.apache.flink.connector.redis.RedisUnsupportedException;
import org.apache.flink.connector.redis.container.RedisContainer;
import org.apache.flink.connector.redis.formatter.RedisStoreStrategy;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamGroupInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * RedisStreamMapper
 *
 * @author Liu Yang
 * @date 2023/1/28 10:21
 */
public class RedisStreamMapper extends RedisRowDataMapper {

    private final String groupName;
    private final String consumerName;
    private final StreamEntryID startId;
    private final ConcurrentHashMap<String, StreamEntryID> streamEntryIDMap = new ConcurrentHashMap<>();

    private volatile boolean running = false;
    private volatile StreamEntryID lastReadEntryId = null;
    private volatile StreamEntryID lastWritEntryId = null;

    public RedisStreamMapper() {
        this(null, null);
    }

    public RedisStreamMapper(String groupName, StreamEntryID id) {
        this.groupName = groupName;
        this.startId = id != null ? id :
                (groupName != null ? StreamEntryID.UNRECEIVED_ENTRY : StreamEntryID.LAST_ENTRY);
        this.consumerName = groupName == null ? null : groupName + "_" + System.currentTimeMillis();
    }

    public StreamEntryID getLastReadEntryId() {
        return lastReadEntryId;
    }

    public StreamEntryID getLastWritEntryId() {
        return lastWritEntryId;
    }

    @Override
    protected void initialize() {
        super.initialize();
    }

    public void close() {
        this.running = false;
    }

    @Override
    public GenericRowData read(RedisContainer container, String key) throws IOException {
        if (key == null) {
            return null;
        }
        List<Map.Entry<String, List<StreamEntry>>> entries;
        try {
            if (groupName == null) {
                entries = container.xread(XReadParams.xReadParams().block(0).count(1), streamEntryIDMap);
            } else {
                entries = container.xreadGroup(groupName, consumerName, XReadGroupParams.xReadGroupParams().block(0) ,streamEntryIDMap);
            }
            lastReadEntryId = entries.get(0).getValue().get(0).getID();
            Map<String, String> results = entries.get(0).getValue().get(0).getFields();
            // 删除接收的流入口标识
            container.xdel(key, lastReadEntryId);
            // 返回结果
            StringData pk = StringData.fromString(lastReadEntryId.toString());
            RowData row = this.convertToRowData(results);
            return GenericRowData.of(pk, row);
        } catch (Exception e){
            throw new IOException("Query redis stream data error at " + key + ".", e);
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
        final String id = row.getString(0).toString();
        if (resultType.getTypeRoot() == LogicalTypeRoot.ROW) {
            rowData = row.getRow(1, fieldNum);
            hash = this.convertToMap(rowData);
        } else {
            throw new RedisUnsupportedException("Not supported [" + key + "] non-row type row[1]: " + row);
        }
        try {
            // 检测是否设置 StreamEntryID
            // StreamEntryID 格式为 时间戳-序号
            final XAddParams xAddParams;
            if (id == null || id.length() == 0) {
                xAddParams = XAddParams.xAddParams();
            } else {
                xAddParams = XAddParams.xAddParams().id(new StreamEntryID(id));
            }
            // 写入数据
            lastWritEntryId = container.xadd(key, xAddParams, hash);
            if (keyTtl != null && keyTtl > 0) {
                container.expire(key, keyTtl);
            }
        } catch (Exception e) {
            throw new IOException("Upsert redis stream data error , cause: " + e.getMessage(), e);
        }
    }

    @Override
    public RowData query(RedisContainer container, String key) throws IOException {
        initialize();
        return read(container, additionalKey);
    }

    @Override
    public void upsert(RedisContainer container, String key, RowData value) throws IOException {
        initialize();
        write(container, additionalKey, value);
    }

    @Override
    public void scan(RedisContainer container, ScanParams params, Consumer<RowData> consumer) throws IOException {
        // TODO: 不支持 scan 方法
        throw new RedisUnsupportedException("Unsupported scan operation.");
    }

    @Override
    public void stream(RedisContainer container, Consumer<RowData> consumer) throws IOException {
        try {
            if (!streamEntryIDMap.containsKey(additionalKey)) {
                streamEntryIDMap.put(additionalKey, startId);
            }
            if (groupName != null) {
                // 尝试创建消费者组
                List<StreamGroupInfo> groups = container.xinfoGroups(additionalKey);
                if (groups.stream().noneMatch(e -> groupName.equals(e.getName()))) {
                    container.xgroupCreate(additionalKey, groupName, StreamEntryID.LAST_ENTRY, true);
                }
            }
        } catch (Exception e) {
            throw e;
        }
        // 开始接收数据
        this.running = true;
        while(running) {
            RowData value = this.query(container, additionalKey);
            if (consumer != null) {
                consumer.accept(value);
            }
        }
    }

    @Override
    public RedisStoreStrategy getRedisStoreStrategy() {
        return RedisStoreStrategy.STREAM;
    }

}
