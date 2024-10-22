package com.example.demo.bigdata.tutorial.flink.chapter4_window.task4;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * @Description: 增量聚合函数和全窗口函数结合
 * @Author: Chenyang on 2024/10/22 21:07
 * @Version: 1.0
 */
public class FlinkWindowTest4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 无延迟水位线
        SingleOutputStreamOperator<SourceEvent2> dataStream = env.addSource(EventUtils.getSourceFunction2())
                .assignTimestampsAndWatermarks(EventUtils.getMyPeriodicWatermarkStrategy());

        // 输出数据
        dataStream.print("input");

        // 每十秒统计一次使用人数
        dataStream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                // 增量聚合函数实时计算, 全窗口函数窗口计算时处理
                .aggregate(new MyAggregateFunction(), new MyProcessWindowFunction())
                .print();

        System.out.println("=====任务提交=====");
        env.execute();

    }

    public static class MyAggregateFunction implements AggregateFunction<SourceEvent2, HashSet<String>, Long> {
        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(SourceEvent2 value, HashSet<String> accumulator) {
            accumulator.add(value.getName());
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long) accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long startTime = context.window().getStart();
            long endTime = context.window().getEnd();
            Long size = elements.iterator().next();
            out.collect("窗口: [" + new Timestamp(startTime) + " ~ " + new Timestamp(endTime) + "] UV值: " + size);
        }
    }
}
