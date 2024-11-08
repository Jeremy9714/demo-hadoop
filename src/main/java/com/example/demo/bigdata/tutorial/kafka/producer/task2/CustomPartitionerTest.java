package com.example.demo.bigdata.tutorial.kafka.producer.task2;

import com.example.demo.bigdata.tutorial.kafka.common.KafkaUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Properties;

/**
 * @Description: kafka 自定义分区
 * @Author: Chenyang on 2024/11/04 12:05
 * @Version: 1.0
 */
public class CustomPartitionerTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.KAFKA_ADDR);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaUtils.KEY_SER);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaUtils.VALUE_SER);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.demo.bigdata.tutorial.kafka.producer.task2.MyPartitioner");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; ++i) {
            producer.send(new ProducerRecord<>("tpc-1", "test-" + i), (recordMetadata, e) -> System.out.println("topic: " + recordMetadata.topic() + ", partition: " + recordMetadata.partition()));
        }
        
        producer.close();
    }

    public static class MyPartitioner implements Partitioner {
        @Override
        public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
            String value = o1.toString();
            if (value.contains("test")) {
                return 0;
            } else {
                return 1;
            }
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> map) {

        }
    }
}
