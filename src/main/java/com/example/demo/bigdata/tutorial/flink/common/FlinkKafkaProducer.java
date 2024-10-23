package com.example.demo.bigdata.tutorial.flink.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Description: kafka producer
 * @Author: Chenyang on 2024/10/23 9:34
 * @Version: 1.0
 */
public class FlinkKafkaProducer {
    public static void main(String[] args) throws InterruptedException {

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop204:9092");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProps);
        int vc = 0;
        while (true) {
            kafkaProducer.send(new ProducerRecord<>("from-flink", "1001," + ++vc + "," + System.currentTimeMillis()));

            Thread.sleep(1000L);
        }
    }
}
