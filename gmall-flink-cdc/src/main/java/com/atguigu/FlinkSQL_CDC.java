package com.atguigu;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class FlinkSQL_CDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. DDL方式建表
        tableEnv.executeSql("CREATE TABLE mysql_binlog (" +
                " tm_id STRING," +
                " tm_name STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'hdp103'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '199037'," +
                " 'database-name' = 'gmall'," +
                " 'table-name' = 'base_trademark'" +
                ")");
        //3. 查询数据
        Table table = tableEnv.sqlQuery("select * from mysql_binlog");

        //4. 将动态表转化为流
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        env.execute("flinkCDCWithSQL");
    }
}
