package com.alibaba.otter.node.etl.common.db.dialect.es;

import com.alibaba.otter.node.etl.common.db.dialect.NosqlTemplate;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import com.taobao.tddl.dbsync.binlog.CustomColumnType;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ElasticsearchTemplate implements NosqlTemplate {
	
	private final Logger logger = LoggerFactory.getLogger(ElasticsearchTemplate.class);
	
	private TransportClient client;
	
    public ElasticsearchTemplate(TransportClient client) {
		super();
		this.client = client;
	}

    @Override
    public List<EventData> batchEventDatas(List<EventData> events, int[] affects) {
    	List<EventData> successEventDatas = new ArrayList<EventData>();
    	for(int i = 0; i < events.size(); i++) {
    		try {
				EventData event = events.get(i);
				if(event.getEventType().isInsert()) {
					this.insertEventData(event);
				} else if(event.getEventType().isUpdate()) {
					this.updateEventData(event);
				} else if(event.getEventType().isDelete()) {
					this.deleteEventData(event);
				}
				affects[i] = 1;
				successEventDatas.add(event);
			} catch (Exception e) {
				logger.error("batch event data exception, event : {}", events.get(i), e);
				affects[i] = 0;
			}
    	}
        return successEventDatas;
    }

    @Override
    public EventData insertEventData(EventData event) {
    	logger.warn("insert event data : {}", event);
        return index(event);
    }

    private EventData index(EventData event){
        XContentBuilder contentBuilder = parseColumns(event);
        String tableId = getTableId(event);
        client.prepareIndex(event.getSchemaName(), event.getTableName(), tableId).setSource(contentBuilder).get();
        return event;
    }

    XContentBuilder parseColumns(EventData event) {
        List<EventColumn> columns = new ArrayList<EventColumn>();
        columns.addAll(event.getKeys());
        columns.addAll(event.getColumns());
        try {
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject();

            // 判断index（对应数据库名）是否存在
            IndicesExistsResponse indexExistsResp = client.admin().indices().exists(new IndicesExistsRequest(new String[]{event.getSchemaName()})).actionGet();
            TypesExistsResponse typeExistsResp = null;
            if(indexExistsResp.isExists()){
                // 判断type（对应表名）是否存在
                typeExistsResp = client.admin().indices().typesExists(new TypesExistsRequest(new String[]{event.getSchemaName()}, event.getTableName())).actionGet();
            }
            CreateIndexRequestBuilder prepareCreate = null;
            XContentBuilder mapping = null;
            /**
             * 此处分为几种情况
             * 1. index不存在，
             * 2. index存在但type不存在
             * 3. index存在并且type存在
             * 只有当两者都存在时（即第3种情况），才不执行mapping的创建
             */
            boolean shouldCreateMapping = !(indexExistsResp.isExists() && typeExistsResp.isExists());
            if(shouldCreateMapping){
                prepareCreate = client.admin().indices().prepareCreate(event.getSchemaName());
                mapping = XContentFactory.jsonBuilder().startObject()
                        .startObject(event.getTableName())
                        .startObject("properties");
            }

            for(EventColumn column : columns) {
                if (column == null) {
                    continue ;
                }
                if(column.getColumnType() == CustomColumnType.POINT){
                    if(column.isNull()){
                        continue ;
                    }
                    // 处理坐标类型的字段值，example：POINT(28.2789745229671,110.827382004967)
                    StringBuilder point = new StringBuilder(column.getColumnValue());
                    point.delete(0, 6).deleteCharAt(point.length() - 1);
                    contentBuilder.field(column.getColumnName(), point.toString());
                    if(shouldCreateMapping){
                        mapping.startObject(column.getColumnName()).field("type", "geo_point").endObject();
                    }
                } else{
                    contentBuilder.field(column.getColumnName(), column.isNull() ? "" : column.getColumnValue());

                }
            }
            contentBuilder.endObject();

            if(shouldCreateMapping){
                mapping.endObject().endObject().endObject();
                prepareCreate.addMapping(event.getTableName(), mapping);
                prepareCreate.execute().actionGet();
            }

            return contentBuilder;
        } catch (Exception e) {
            logger.error("parse columns exception, event: {}", e, event);
        }
        return null;
    }

    private String getTableId(EventData event){
        StringBuilder tableId = new StringBuilder();
        for(EventColumn column : event.getKeys()) {
            if(tableId.length() > 0) {
                tableId.append("-");
            }
            tableId.append(column.getColumnValue());
        }
        return tableId.toString();
    }
    
    @Override
    public EventData updateEventData(EventData event) {
    	logger.warn("update event data : {}", event);
        return index(event);
    }

    @Override
    public EventData deleteEventData(EventData event) {
    	logger.warn("delete event data : {}", event);
        String tableId = getTableId(event);
        client.prepareDelete(event.getSchemaName(), event.getTableName(), tableId).execute();
        return event;
    }

    @Override
    public EventData createTable(EventData event) {
        return null;
    }

    @Override
    public EventData alterTable(EventData event) {
        return null;
    }

    @Override
    public EventData eraseTable(EventData event) {
        return null;
    }

    @Override
    public EventData truncateTable(EventData event) {
        return null;
    }

    @Override
    public EventData renameTable(EventData event) {
        return null;
    }
}
