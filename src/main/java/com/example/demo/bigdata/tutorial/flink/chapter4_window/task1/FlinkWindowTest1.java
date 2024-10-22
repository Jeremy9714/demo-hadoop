package com.example.demo.bigdata.tutorial.flink.chapter4_window.task1;

import com.example.demo.bigdata.tutorial.flink.common.EventGenerator;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Description: window
 * @Author: Chenyang on 2024/10/22 11:46
 * @Version: 1.0
 */
public class FlinkWindowTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStream<FlinkEvent> dataStream = env.addSource(EventGenerator.getSourceFunction());
        SingleOutputStreamOperator<SourceEvent2> dataStream = env.addSource(EventGenerator.getSourceFunction2())
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<SourceEvent2>forBoundedOutOfOrderness(Duration.ZERO)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SourceEvent2>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<SourceEvent2>) (element, recordTimestamp) -> element.getTimestamp())
                );

//        dataStream.print();

        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = dataStream.map(new MapFunction<SourceEvent2, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(SourceEvent2 value) throws Exception {
                return Tuple2.of(value.getName(), 1L);
            }
        })
//                // 不分区窗口
//                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .keyBy(item -> item.f0)
//                // 计数窗口
//                .countWindow(10, 5)
                // ET滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                // PT滚动窗口
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//                // ET滑动窗口
//                .window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
//                // PT滑动窗口
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(30),Time.seconds(10)))
//                // ET会话窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
//                // PT会话窗口
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
                // windowFunction
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });

        resultStream.print();

        System.out.println("====任务提交====");
        env.execute();

    }
    
}
