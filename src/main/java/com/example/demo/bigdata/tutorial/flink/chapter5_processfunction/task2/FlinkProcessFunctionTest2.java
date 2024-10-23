package com.example.demo.bigdata.tutorial.flink.chapter5_processfunction.task2;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Description: KeyedProcessFunction PT
 * @Author: Chenyang on 2024/10/23 16:37
 * @Version: 1.0
 */
public class FlinkProcessFunctionTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(EventUtils.getSourceFunction2())
                .keyBy(SourceEvent2::getName)
                .process(new MyProcessFunction())
                .print();

        env.execute();
    }

    public static class MyProcessFunction extends KeyedProcessFunction<String, SourceEvent2, String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void processElement(SourceEvent2 value, Context ctx, Collector<String> out) throws Exception {
            String currentKey = ctx.getCurrentKey();
//            Long timestamp = ctx.timestamp();
            long processingTime = ctx.timerService().currentProcessingTime();
//            long watermark = ctx.timerService().currentWatermark();
            ctx.timerService().registerProcessingTimeTimer(processingTime + 10000L);

            // 注册10s后的定时器
            out.collect(currentKey + "数据到达, 到达时间: " + new Timestamp(processingTime));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "定时器触发, 定时时间: " + new Timestamp(timestamp));
        }
    }
}
