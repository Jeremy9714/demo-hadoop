package com.example.demo.bigdata.tutorial.kafka.consumer.task1;

import com.example.demo.bigdata.tutorial.kafka.common.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/04 15:44
 * @Version: 1.0
 */
public class CustomProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.KAFKA_ADDR);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaUtils.KEY_SER);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaUtils.VALUE_SER);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 5; ++i) {
            producer.send(new ProducerRecord<>("tpc-1", 1, "", "record-" + i), (metadata, e) -> {
                System.out.println("topic: " + metadata.topic() + ", partition: " + metadata.partition());
            });
        }

        producer.close();
    }
}
