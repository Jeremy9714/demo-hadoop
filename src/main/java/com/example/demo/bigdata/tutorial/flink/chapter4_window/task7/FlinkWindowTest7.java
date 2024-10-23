package com.example.demo.bigdata.tutorial.flink.chapter4_window.task7;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Description: lateness
 * @Author: Chenyang on 2024/10/23 10:19
 * @Version: 1.0
 */
public class FlinkWindowTest7 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> dataStream = env.addSource(EventUtils.getKafkaSource("hadoop204:9092", "from-flink"))
                .map(EventUtils.getWaterSensorMapper())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs())
                );

        // 定义输出标签
        OutputTag<WaterSensor> outputTag = new OutputTag("lateData", TypeInformation.of(new TypeHint<WaterSensor>() {
        }));

        SingleOutputStreamOperator<String> resultStream = dataStream.keyBy(key -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                // 窗口延迟2s
                .allowedLateness(Time.minutes(1L))
                // 过期时间侧输出流
                .sideOutputLateData(outputTag)
                .aggregate(new MyAggregateFunction(), new MyProcessWindowFunction());


        resultStream.print(">>>");
        // 通过输出标签获取流中迟到数据
        resultStream.getSideOutput(outputTag).print("late");

        System.out.println("=====任务提交=====");
        env.execute();
    }

    public static class MyAggregateFunction implements AggregateFunction<WaterSensor, WaterSensor, WaterSensor> {
        @Override
        public WaterSensor createAccumulator() {
            return new WaterSensor();
        }

        @Override
        public WaterSensor add(WaterSensor value, WaterSensor accumulator) {
            return accumulator.getTs() > value.getTs() ? accumulator : value;
        }

        @Override
        public WaterSensor getResult(WaterSensor accumulator) {
            return accumulator;
        }

        @Override
        public WaterSensor merge(WaterSensor a, WaterSensor b) {
            return null;
        }
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<WaterSensor, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect(new String("窗口: [" + new Timestamp(start) + " ~ " + new Timestamp(end) + "] 最新数据: " + elements.iterator().next()));
        }
    }
}
