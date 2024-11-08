package com.example.demo.bigdata.tutorial.kafka.consumer.task3;

import com.example.demo.bigdata.tutorial.kafka.common.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/04 16:40
 * @Version: 1.0
 */
public class AssignerConsumer2 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.KAFKA_ADDR);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaUtils.KEY_DESER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaUtils.VALUE_DESER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-4");
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 设置分区分配策略
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, KafkaUtils.STICKY);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("tpc-2"));

//        TopicPartition topicPartition = new TopicPartition("tpc-1", 0);
//        consumer.assign(Arrays.asList(topicPartition));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic: " + record.topic() + ", partition: " + record.partition() + ", value: " + record.value());
            }
        }
    }
}
