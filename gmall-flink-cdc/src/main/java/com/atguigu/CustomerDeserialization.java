package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {
    /**
     * 封装的数据格式
     * {
     *     "database":"",
     *     "tableName":"",
     *     "type":"CURD",
     *     "before":{"":"",...},
     *     "after":{"":"",...},
     *     "ts":""
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 1创建JSON对象用于存储最终数据
        JSONObject result = new JSONObject();

        // 2获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct value = (Struct)sourceRecord.value();
        // 3获取before数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null ) {
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for (int i = 0; i < beforeFields.size(); i++) {
                Field field = beforeFields.get(i);
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }


        // 4获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null ) {
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (int i = 0; i < afterFields.size(); i++) {
                Field field = afterFields.get(i);
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }
        // 5获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        // 6将字段写入JSON
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        // 7输出数据
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
