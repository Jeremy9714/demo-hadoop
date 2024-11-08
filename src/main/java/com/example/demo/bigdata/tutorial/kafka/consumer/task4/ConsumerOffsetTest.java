package com.example.demo.bigdata.tutorial.kafka.consumer.task4;

import com.example.demo.bigdata.tutorial.kafka.common.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

/**
 * @Description: 指定分区消费测试
 * @Author: Chenyang on 2024/11/04 17:32
 * @Version: 1.0
 */
public class ConsumerOffsetTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.KAFKA_ADDR);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-6");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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

        // 指定offset消费
        for (TopicPartition topicPartition : assignment) {
            consumer.seek(topicPartition, 100);
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
