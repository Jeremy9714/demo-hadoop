package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task1;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Description: source算子
 * @Author: Chenyang on 2024/10/20 21:27
 * @Version: 1.0
 */
public class FlinkSourceTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        // 批处理模式
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH); 

        // 从集合中读取数据
        List<CustomFlinkEvent> list = Arrays.asList(new CustomFlinkEvent(1, "Joe", 1000L),
                new CustomFlinkEvent(2, "Jean", 2000L));
        DataStream<CustomFlinkEvent> collectionStream = env.fromCollection(list);

        DataStream<CustomFlinkEvent> elementStream = env.fromElements(new CustomFlinkEvent(1, "Joe", 1000L),
                new CustomFlinkEvent(2, "Jean", 2000L));

        // 从kafka中读取数据
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop204:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-group");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        DataStream<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<>("to-flink", new SimpleStringSchema(), props));
        
        // 输出
        collectionStream.print("col1");
        elementStream.print("col2");
        kafkaStream.print("kafka");


        System.out.println("=======提交任务=======");
        env.execute();

    }
}
