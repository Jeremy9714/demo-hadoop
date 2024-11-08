package com.example.demo.bigdata.tutorial.kafka.producer.task1;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @Description: kafka事务
 * @Author: Chenyang on 2024/11/04 9:39
 * @Version: 1.0
 */
public class ProducerTransactionTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop202:9092,hadoop203:9092,hadoop204:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx_1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        producer.initTransactions();
        producer.beginTransaction();

        try {
            for (int i = 0; i < 5; ++i) {
                producer.send(new ProducerRecord<>("tpc-1", "record" + i),
                        (recordMetadata, e) -> System.out.println("主题: " + recordMetadata.topic() + ", 分区: " + recordMetadata.partition()));
                int result = 1/0;
            }
            producer.commitTransaction();
        } catch (Exception e) {
            // 事务回滚
            producer.abortTransaction();
        } finally {
            producer.close();
        }

    }
}
