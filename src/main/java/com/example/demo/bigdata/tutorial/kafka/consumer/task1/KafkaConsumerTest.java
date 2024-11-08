package com.example.demo.bigdata.tutorial.kafka.consumer.task1;

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
 * @Description: consumer消费
 * @Author: Chenyang on 2024/11/04 15:44
 * @Version: 1.0
 */
public class KafkaConsumerTest {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.KAFKA_ADDR);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaUtils.KEY_DESER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaUtils.VALUE_DESER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
//        // 订阅主题
//        consumer.subscribe(Arrays.asList("tpc-1"));

        // 指定分区消费
        TopicPartition topicPartition = new TopicPartition("tpc-1", 1);
        consumer.assign(Arrays.asList(topicPartition));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("消费【主题: " + record.topic() + ", partition: " + record.partition() + ", value: " + record.value() + "】");
            }
        }
    }
}
