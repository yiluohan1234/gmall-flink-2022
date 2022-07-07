package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

//数据流：web/app -> Nginx -> SpringBoot -> Kafka（ods）-> FLinkApp -> Kafka（dwd）
//程  序：mockLog -> Nginx ->Logger.sh -> Kafka(ZK) -> BaseLogApp  -> kafka
public class BaseLogApp {
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

        //ToDo 2.消费ods_base_og主愿数据创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //ToDo 3.将每行数据转换为切S0N对象
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSONObject::parseObject);
        OutputTag<String> outputTag = new OutputTag<String>("Dirty"){
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s); // option + command + t
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    //发生异常，将数据写入侧输出流
                    context.output(outputTag, s);
                }
            }
        });

        //打印脏数据
        jsonObjDS.getSideOutput(outputTag).print("Dirty>>>>>>>>>>>");

        //ToDo 4.新老用户校验 状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    //声明状态用于表示当前 Mid 是否已经访问过
                    private ValueState<String> valueState;
                    private SimpleDateFormat simpleDateFormat;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        //获取数据中的"is new"标记
                        String isNew = value.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) {
                            //获取状态数据
                            String state = valueState.value();
                            Long ts = value.getLong("ts");
                            if (state != null) {
                                //修改sNew标记
                                value.getJSONObject("common").put("is_new", 0);
                            } else {
                                //更新状态
                                valueState.update("1");
                                valueState.update(simpleDateFormat.format(ts));
                            }
                        }
                        //返回数据
                        return value;
                    }
                });

        //ToDo 5.分流 侧输出流    页面：主流   启动：侧输出流 堡光：侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                //获取启动日志字段
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    //将数据写入启动日志侧输出流
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    //将数据写入页面日志主流
                    collector.collect(jsonObject.toJSONString());

                    //取出数据中的曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {

                        //获取页面ID
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            //添加页面
                            display.put("page_id", pageId);

                            //将输出写出到曝光侧输出流
                            context.output(displayTag, display.toJSONString());

                        }
                    }
                }
            }
        });
        //ToDo 6.提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        //ToDo 7.将三个流进行打印并输出到对应的Kāfka主题中
        startDS.print("Start>>>>>>>>>>>");
        pageDS.print("Page>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>>>");

        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //T0D0 8.启动任务
        env.execute("BaseLogApp");
    }
}
