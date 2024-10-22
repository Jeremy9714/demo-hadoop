package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task4;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent1;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description: 分区、聚合
 * @Author: Chenyang on 2024/10/21 11:20
 * @Version: 1.0
 */
public class FlinkTransformationTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SourceEvent1> dataStream = env.fromCollection(EventUtils.getEvent1List());

        // keyBy
        // max 只更新最大值字段
        dataStream.keyBy(new MyKeySelector())
                .max("timestamp")
                .print("max: ");

        // maxBy 更新整条数据
        dataStream.keyBy(item -> item.getName())
                .maxBy("timestamp")
                .print("maxBy: ");

        System.out.println("=====任务提交=====");
        env.execute();
    }

    public static class MyKeySelector implements KeySelector<SourceEvent1, String> {
        @Override
        public String getKey(SourceEvent1 value) throws Exception {
            return value.getName();
        }
    }
}
