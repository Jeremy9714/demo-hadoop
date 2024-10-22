package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task5;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description: 聚合
 * @Author: Chenyang on 2024/10/21 11:55
 * @Version: 1.0
 */
public class FlinkTransformationTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SourceEvent2> dataStream = env.fromCollection(EventUtils.getEvent2List());

        // 聚合获取总和
        SingleOutputStreamOperator<Tuple2<String, Long>> reducedStream = dataStream.map(item -> Tuple2.of(item.getName(), 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {})
                .keyBy(item -> item.f0)
                .reduce(new MyReduceFunction());

        // 获取最大值
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = reducedStream.keyBy(item -> "key")
                .reduce((Tuple2<String, Long> value1, Tuple2<String, Long> value2) -> value1.f1 > value2.f1 ? value1 : value2);

        resultStream.print();

        System.out.println("=====任务提交=====");
        env.execute();
    }

    public static class MyReduceFunction implements ReduceFunction<Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
        }
    }
}
