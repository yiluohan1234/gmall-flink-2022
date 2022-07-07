package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm)
//程  序：mockLog -> Nginx -> Logger.sh  -> Kafka(ZK)  -> BaseLogApp -> kafka -> UserJumpDetailApp -> Kafka
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //ToDo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 生产环境，与Kafka分区数保持一致

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
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //ToDo 3.将每行数据转换为JSON对象并提取时间戳生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }));

        //ToDo 4.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

        //使用循环模式  定义模式序列
        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        })
                .times(2)
                .consecutive() //指定严格近邻(next)
                .within(Time.seconds(10));

        //ToDo 5.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP
                .pattern(jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid")), pattern);

        //ToDo 6.提取匹配上的和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeOut") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long ts) throws Exception {
                        return map.get("start").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                });
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);


        //ToDo 7.UNION两种事件
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        //ToDo 8.将数据写入kafka
        unionDS.print();
        unionDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //ToDo 9.启动任务
        env.execute("UserJumpDetailApp");

    }
}
