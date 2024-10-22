package com.example.demo.bigdata.tutorial.flink.chapter4_window.task2;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;

/**
 * @Description: window aggregate
 * @Author: Chenyang on 2024/10/22 17:24
 * @Version: 1.0
 */
public class FlinkWindowTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 水位线5s延迟
        DataStream<SourceEvent2> dataStream = env.addSource(EventUtils.getSourceFunction2())
                .assignTimestampsAndWatermarks(new EventUtils.MyWatermarkStrategy(new EventUtils.MyPeriodicWatermarkGenerator()));

        dataStream.keyBy(SourceEvent2::getName)
                // 10s滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                // 统计每个用户ts平均值
                .aggregate(new MyAggregateFunction())
                .print();

        System.out.println("=====任务提交=====");
        env.execute();
    }

    // 自定义聚合函数
    public static class MyAggregateFunction implements AggregateFunction<SourceEvent2, Tuple3<String, Integer, Long>, String> {

        @Override
        public Tuple3<String, Integer, Long> createAccumulator() {
            return Tuple3.of("", 0, 0L);
        }

        @Override
        public Tuple3<String, Integer, Long> add(SourceEvent2 value, Tuple3<String, Integer, Long> accumulator) {
            return Tuple3.of(value.getName(), accumulator.f1 + 1, accumulator.f2 + value.getTimestamp());
        }

        @Override
        public String getResult(Tuple3<String, Integer, Long> accumulator) {
            Timestamp avgTimestamp = new Timestamp(accumulator.f2 / accumulator.f1);
            return accumulator.f0 + ": " + avgTimestamp.toString();
        }

        @Override
        public Tuple3<String, Integer, Long> merge(Tuple3<String, Integer, Long> a, Tuple3<String, Integer, Long> b) {
            return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}
