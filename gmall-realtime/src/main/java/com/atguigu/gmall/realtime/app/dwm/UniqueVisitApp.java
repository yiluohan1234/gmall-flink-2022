package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

//数据流：web/app -> Nginx -> SpringBoot -> Kafka（ods）-> FLinkApp -> Kafka（dwd） -> FlinkApp -> kafka(dwm)
//程  序：mockLog -> Nginx ->Logger.sh -> Kafka(ZK) -> BaseLogApp  -> kafka -> UniqueVisitApp -> kafka
public class UniqueVisitApp {

    public static void main(String[] args) throws Exception {
        //ToDo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 设置CK&状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hdp101:8020/gmall-flink-210325/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        //ToDo 2.读Kafka dwd page Log主题的数据
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //ToDo 3.将每行数据转换切JS0N对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);


        //ToDo 4.过滤数据状态编程只保留每个mid每天第一次登陆的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> dateState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueValueStateDescriptor = new ValueStateDescriptor<>
                        ("date-state", String.class);
                // 设置状态的超时时间以及更新时间的方式
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueValueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dateState = getRuntimeContext().getState(valueValueStateDescriptor);

                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                // 取出上一条页面信息
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                // 判断上一条页面是否为NULL
                if (lastPageId == null || lastPageId.length() <= 0) {
                    // 取出状态数据
                    String lastDate = dateState.value();

                    // 取出今天的日期
                    String curDate = simpleDateFormat.format(jsonObject.getLong("ts"));

                    // 判断连个日期是否相同
                    if (!curDate.equals(lastDate)) {
                        dateState.update(curDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        //ToDo 5.将数据写入kafka
        uvDS.print();
        uvDS.map(json -> json.toJSONString()).addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //ToDo 6.启动任务
        env.execute("UniqueVisitApp");
    }

}
