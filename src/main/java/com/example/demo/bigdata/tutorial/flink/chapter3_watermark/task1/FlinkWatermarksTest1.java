package com.example.demo.bigdata.tutorial.flink.chapter3_watermark.task1;

import com.example.demo.bigdata.tutorial.flink.common.EventGenerator;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Description: watermark
 * @Author: Chenyang on 2024/10/21 21:59
 * @Version: 1.0
 */
public class FlinkWatermarksTest1 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置、获取watermark生成周期(ms), 默认200ms
        long autoWatermarkInterval = env.getConfig().getAutoWatermarkInterval();
        System.out.println("=====默认水位线生成周期: " + autoWatermarkInterval + "ms=====");
        env.getConfig().setAutoWatermarkInterval(100);

        // 有序流
        env.fromCollection(EventGenerator.getEvent2List())
                // 有序流生成策略
                .assignTimestampsAndWatermarks(
                        // 设置WatermarkGenerator生成watermark逻辑
                        WatermarkStrategy.<SourceEvent2>forMonotonousTimestamps()
                                // 设置TimestampAssigner提取时间戳逻辑
                                .withTimestampAssigner(new SerializableTimestampAssigner<SourceEvent2>() {
                                    @Override
                                    public long extractTimestamp(SourceEvent2 element, long recordTimestamp) {
                                        // 毫秒数
                                        return element.getTimestamp();
                                    }
                                })
                );

        // 乱序流
        env.fromCollection(EventGenerator.getEvent2List())
                // 乱序留生成策略
                .assignTimestampsAndWatermarks(
                        // 设置WatermarkGenerator生成watermark逻辑
                        WatermarkStrategy.<SourceEvent2>forBoundedOutOfOrderness(Duration.ofSeconds(2L)) // 设置延迟时间
                                .withTimestampAssigner(new SerializableTimestampAssigner<SourceEvent2>() {
                                    @Override
                                    public long extractTimestamp(SourceEvent2 element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                })
                );

    }
}
