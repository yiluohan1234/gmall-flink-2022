package com.atguigu.gmall.realtime.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection = null;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 连接 phoenix
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        // 1获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String key = jsonObject.getString("tableName") + "-" + jsonObject.getString("type");
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {

            // 2过滤字段
            JSONObject data = jsonObject.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            // 3分流
            // 将输出表/主题信息写入value
            jsonObject.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //kafka数据,写入主流
                collector.collect(jsonObject);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //Hbase数据,写入侧输出流
                readOnlyContext.output(outputTag, jsonObject);
            }
        } else {
            System.out.println("该组合Key：" + key + "不存在！");
        }


    }

    /**
     * @param data        {"id":"11","tm_name":"atguigu","logo_url":"aaa"}
     * @param sinkColumns id,tm_name
     *                    {"id":"11","tm_name":"atguigu"}
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);

//        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
//        while (iterator.hasNext()){
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columns.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));

    }

    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
        // 1获取并解析数据
        JSONObject jsonObject = JSON.parseObject(s);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);


        // 2建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        // 3写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    //建表语句 : create table if not exists db.tn(id varchar primary key,tm_name varchar) xxx;
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];

                // 判断是否为主键
                if (sinkPk.equals(field)) {
                    createTableSQL.append(field).append(" varchar primary key ");
                } else {
                    createTableSQL.append(field).append(" varchar");
                }

                // 判断是否为最后一个字段，如果不是，则添加","
                if (i < fields.length - 1) {
                    createTableSQL.append(",");
                }

            }

            createTableSQL.append(")").append(sinkExtend);

            // 打印建表语句
            System.out.println(createTableSQL);

            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());

            //执行
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
