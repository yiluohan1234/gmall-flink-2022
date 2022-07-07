package com.atguigu;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCWithCustomerDeserialization {
    public static void main(String[] args) throws Exception {
        //1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 通过FlinkCDC构建sourceFunction并读取数据
        //initial (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
        //latest-offset: Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.
        //timestamp: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified timestamp. The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
        //specific-offset: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified offset.
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hdp103")
                .port(3306)
                .username("root")
                .password("199037")
                .databaseList("gmall")
                .tableList("gmall.base_trademark") //如果不添加该参数，则消费指定数据库中所有表的数据。如果指定，指定方式为db.tablename
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        // 3. 打印数据
        streamSource.print();
        // 4. 启动任务
        env.execute("FlinkCDCWithCustomerDeserialization");
    }
}
