package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    private static String default_topic = "dwd_default_topic";

    private static String brokers = "hdp101:9092,hdp102:9092,hdp103:9092";
    private static Properties properties = new Properties();


    /**
     * 获取 kafkaProducer 的方法
     *
     * @param topic 主题
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);

    }

    /**
     * addsink
     * @param kafkaSerializationSchema 泛型的Serialization
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
//        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5 * 60 * 1000 + "");

        return new FlinkKafkaProducer<T>(default_topic,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); //CK 开启才有用 EXACTLY_ONCE
    }

    /**
     * 获取 KafkaConsumer 的方法
     *
     * @param topic 主题
     * @param groupId 消费者组
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);

    }
}
