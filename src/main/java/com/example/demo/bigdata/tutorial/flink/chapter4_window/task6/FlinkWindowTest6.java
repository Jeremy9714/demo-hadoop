package com.example.demo.bigdata.tutorial.flink.chapter4_window.task6;

import com.example.demo.bigdata.tutorial.flink.common.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Description: trigger
 * @Author: Chenyang on 2024/10/23 9:18
 * @Version: 1.0
 */
public class FlinkWindowTest6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty("bootstrap.servers", "hadoop204:9092");
        consumerConfig.setProperty("group.id", "flink-group");
        consumerConfig.setProperty("auto.offset.reset", "latest");
        consumerConfig.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        SingleOutputStreamOperator<WaterSensor> dataStream = env.addSource(new FlinkKafkaConsumer<>("from-flink", new SimpleStringSchema(), consumerConfig))
                .map(line -> {
                    String[] words = line.split(",");
                    return new WaterSensor(words[0], Double.parseDouble(words[1]), Long.parseLong(words[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((element, ts) -> (element.getTs())));

        dataStream.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new MyTrigger())
                .maxBy("vc")
                .print(">>>");

        System.out.println("=====任务提交=====");
        env.execute();
    }

    // 自定义触发器
    public static class MyTrigger extends Trigger<WaterSensor, TimeWindow> {
        @Override
        public TriggerResult onElement(WaterSensor element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 水位线超过窗口
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                // 触发timer
                return TriggerResult.FIRE;
            } else {
                ctx.registerEventTimeTimer(window.maxTimestamp());
                ctx.registerProcessingTimeTimer(window.maxTimestamp() + 10000L);
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // 清除事件时间timer
            ctx.deleteEventTimeTimer(window.maxTimestamp());
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            if (time == window.maxTimestamp()) {
                // 清除
                ctx.deleteProcessingTimeTimer(window.maxTimestamp() + 10000L);
                return TriggerResult.FIRE;
            } else {
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.maxTimestamp());
            ctx.deleteProcessingTimeTimer(window.maxTimestamp() + 10000L);
        }
    }
}
