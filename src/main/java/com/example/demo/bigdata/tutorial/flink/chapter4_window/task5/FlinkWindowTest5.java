package com.example.demo.bigdata.tutorial.flink.chapter4_window.task5;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import com.example.demo.bigdata.tutorial.flink.common.UrlCountEntity;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Description: window用例
 * @Author: Chenyang on 2024/10/22 21:34
 * @Version: 1.0
 */
public class FlinkWindowTest5 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SourceEvent2> dataStream = env.addSource(EventUtils.getSourceFunction2())
                .assignTimestampsAndWatermarks(EventUtils.getMyPeriodicWatermarkStrategy());

        dataStream.map(item -> new String(item.getUrl() + ", " + new Timestamp(item.getTimestamp()))).print("input");

        // 统计10秒内url浏览量
        dataStream.keyBy(SourceEvent2::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .aggregate(new MyAggregateFunction(), new MyProcessWindowFunction())
                .print();

        System.out.println("=====任务提交=====");
        env.execute();
    }

    public static class MyAggregateFunction implements AggregateFunction<SourceEvent2, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(SourceEvent2 value, Long accumulator) {
            return ++accumulator;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Long, UrlCountEntity, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UrlCountEntity> out) throws Exception {
            TimeWindow window = context.window();
            out.collect(new UrlCountEntity(s, elements.iterator().next(), window.getStart(), window.getEnd()));
        }
    }
}
