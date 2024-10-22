package com.example.demo.bigdata.tutorial.flink.chapter4_window.task3;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

/**
 * @Description: window WindowFunction
 * @Author: Chenyang on 2024/10/22 20:20
 * @Version: 1.0
 */
public class FlinkWindowTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SourceEvent2> dataStream = env.addSource(EventUtils.getSourceFunction2())
                .assignTimestampsAndWatermarks(new EventUtils.MyWatermarkStrategy(new EventUtils.MyPeriodicWatermarkGenerator()));

        dataStream.keyBy(key -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new MyProcessWindowFunction())
                .print();

        System.out.println("=====任务提交=====");
        env.execute();
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<SourceEvent2, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<SourceEvent2> elements, Collector<String> out) throws Exception {
            Set<String> names = new HashSet<>();
            for (SourceEvent2 element : elements) {
                names.add(element.getName());
            }

            int size = names.size();

            // 获取当前窗口
            TimeWindow window = context.window();
            // 起始时间
            long start = window.getStart();
            // 结束时间
            long end = window.getEnd();

            out.collect("窗口: [" + new Timestamp(start) + " ~ " + new Timestamp(end) + "] UV值: " + size);
        }
    }
}
