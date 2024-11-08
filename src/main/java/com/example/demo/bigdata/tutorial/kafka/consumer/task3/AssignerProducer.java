package com.example.demo.bigdata.tutorial.kafka.consumer.task3;

import com.example.demo.bigdata.tutorial.kafka.common.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/04 16:11
 * @Version: 1.0
 */
public class AssignerProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.KAFKA_ADDR);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaUtils.KEY_SER);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaUtils.VALUE_SER);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            for (int i = 0; i < 500; ++i) {
                producer.send(new ProducerRecord<>("tpc-2", "record-" + i), (record, e) -> {
                    System.out.println("topic: " + record.topic() + ", partition: " + record.partition());
                });
                
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
