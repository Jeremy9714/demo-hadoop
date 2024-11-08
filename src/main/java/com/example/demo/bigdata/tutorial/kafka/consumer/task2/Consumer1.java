package com.example.demo.bigdata.tutorial.kafka.consumer.task2;

import com.example.demo.bigdata.tutorial.kafka.common.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/04 16:04
 * @Version: 1.0
 */
public class Consumer1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.KAFKA_ADDR);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaUtils.KEY_DESER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaUtils.VALUE_DESER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-2");
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("tpc-1"));

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
