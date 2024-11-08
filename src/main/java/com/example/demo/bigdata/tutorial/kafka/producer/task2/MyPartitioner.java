package com.example.demo.bigdata.tutorial.kafka.producer.task2;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/04 12:19
 * @Version: 1.0
 */
public class MyPartitioner implements Partitioner {
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