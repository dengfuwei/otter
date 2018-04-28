package com.alibaba.otter.node.etl.common.db.dialect.es;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.node.etl.common.db.dialect.NosqlTemplate;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;

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
				logger.error("batch event data exception, event : {}", e, events.get(i));
				affects[i] = 0;
			}
    	}
        return successEventDatas;
    }

    @Override
    public EventData insertEventData(EventData event) {
    	logger.warn("insert event data : {}", event);
    	Map<String, Object> dataMap = parseColumnsToMap(event);
    	StringBuilder tableId = new StringBuilder();
    	for(EventColumn column : event.getKeys()) {
    		if(tableId.length() > 0) {
    			tableId.append("-");
    		}
    		tableId.append(column.getColumnValue());
    	}
    	client.prepareIndex(event.getSchemaName(), event.getTableName(), tableId.toString()).setSource(dataMap, XContentType.JSON).get();
        return event;
    }
    
    Map<String, Object> parseColumnsToMap(EventData event) {
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        List<EventColumn> columns = new ArrayList<EventColumn>();
        columns.addAll(event.getKeys());
        columns.addAll(event.getColumns());
        for(EventColumn column : columns) {
        	if (column == null) {
                continue ;
            }
            jsonMap.put(column.getColumnName(), column.isNull() ? null : column.getColumnValue());
        }
        return jsonMap;
    }
    
    @Override
    public EventData updateEventData(EventData event) {
    	logger.warn("update event data : {}", event);
        return null;
    }

    @Override
    public EventData deleteEventData(EventData event) {
    	logger.warn("delete event data : {}", event);
        return null;
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
