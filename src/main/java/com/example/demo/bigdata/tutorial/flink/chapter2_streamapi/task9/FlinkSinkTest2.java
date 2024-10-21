package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task9;

import com.example.demo.bigdata.tutorial.flink.common.FlinkEvent;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Description: kafka sink
 * @Author: Chenyang on 2024/10/21 16:57
 * @Version: 1.0
 */
public class FlinkSinkTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty("bootstrap.servers", "hadoop204:9092");
        consumerConfig.setProperty("group.id", "flink-group");
        consumerConfig.setProperty("auto.offset.reset", "latest");
        consumerConfig.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<>("to-flink", new SimpleStringSchema(), consumerConfig));

        SingleOutputStreamOperator<String> resultStream = kafkaStream.map(item -> {
            FlinkEvent event = new FlinkEvent();
            String[] words = item.split(",");
            event.setId(words[0]);
            event.setName(words[1]);
            event.setAge(Integer.parseInt(words[2]));
            System.out.println("=====" + event + "=====");
            return event.toString();
        });

        Properties producerConfig = new Properties();
        producerConfig.setProperty("bootstrap.servers", "hadoop204:9092");
        resultStream.addSink(new FlinkKafkaProducer<>("from-flink", new SimpleStringSchema(), producerConfig));

        System.out.println("=====任务提交=====");
        env.execute();

    }
}
