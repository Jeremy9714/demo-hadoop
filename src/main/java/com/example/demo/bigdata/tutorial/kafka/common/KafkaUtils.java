package com.example.demo.bigdata.tutorial.kafka.common;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/04 12:11
 * @Version: 1.0
 */
public class KafkaUtils {
    public static final String KAFKA_ADDR = "hadoop202:9092,hadoop203:9092,hadoop204:9092";
    public static final String KEY_SER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String VALUE_SER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KEY_DESER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String VALUE_DESER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String RANGE = "org.apache.kafka.clients.consumer.RangeAssignor";
    public static final String ROUND_ROBIN = "org.apache.kafka.clients.consumer.RoundRobinAssignor";
    public static final String STICKY = "org.apache.kafka.clients.consumer.StickyAssignor";
    public static final String COOPERATIVE_STICKY = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor";
}
