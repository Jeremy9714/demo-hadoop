package com.example.demo.bigdata.tutorial.kafka.consumer.task4;

import com.example.demo.bigdata.tutorial.kafka.common.KafkaUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @Description: 指定事件消费测试
 * @Author: Chenyang on 2024/11/04 17:32
 * @Version: 1.0
 */
public class ConsumerTimeOffsetTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.KAFKA_ADDR);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-7");
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 订阅topic
        consumer.subscribe(Arrays.asList("tpc-1"));

        // 获取分区方案
        Set<TopicPartition> assignment = consumer.assignment();

        // 重分区等待
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofSeconds(1L));
            assignment = consumer.assignment();
        }

        // 将指定时间转换为对应offset
        Map<TopicPartition, Long> map = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
            map.put(topicPartition, System.currentTimeMillis() - 24 * 60 * 60 * 1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = consumer.offsetsForTimes(map);

        // 指定时间对应的offset消费
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(topicPartition);
            consumer.seek(topicPartition, offsetAndTimestamp.offset());
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1L));
            for (ConsumerRecord<String, String> record : records) {
//                System.out.println("主题: " + record.topic() + ", 分区: " + record.partition() + ", 位移: " + record.offset());
                System.out.println(record);
            }
        }
    }
}
