package com.example.demo.bigdata.tutorial.flink.chapter5_processfunction.task3;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Description: KeyedProcessFunction ET
 * @Author: Chenyang on 2024/10/23 16:58
 * @Version: 1.0
 */
public class FlinkProcessFunctionTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.addSource(EventUtils.getSourceFunction2())
        env.addSource(new MySourceFunction())
                .assignTimestampsAndWatermarks(EventUtils.getMyPeriodicWatermarkStrategy(0L))
                .keyBy(SourceEvent2::getName)
                .process(new MyKeyedProcessFunction())
                .print();

        env.execute();
    }

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<String, SourceEvent2, String> {

        @Override
        public void processElement(SourceEvent2 value, Context ctx, Collector<String> out) throws Exception {
            out.collect("[" + getRuntimeContext().getIndexOfThisSubtask() + "]" + ctx.getCurrentKey() + " 数据到达, 时间戳为: " + new Timestamp(ctx.timestamp()) + ", watermark: " + ctx.timerService().currentWatermark());

            // 注册10s后的ET定时器
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("[" + getRuntimeContext().getIndexOfThisSubtask() + "]" + ctx.getCurrentKey() + " 定时器触发, 触发时间: " + new Timestamp(timestamp) + ", watermark: " + ctx.timerService().currentWatermark());
        }
    }

    public static class MySourceFunction implements SourceFunction<SourceEvent2> {
        @Override
        public void run(SourceContext<SourceEvent2> ctx) throws Exception {
            // 直接输出数据
            ctx.collect(new SourceEvent2("Jeremy", "/home", 1000L));
            Thread.sleep(5000L);

            // watermark为事件时间-1ms
            ctx.collect(new SourceEvent2("Sean", "/home", 11000L));
            Thread.sleep(5000L);

            ctx.collect(new SourceEvent2("Jason", "/home", 11001L));
            Thread.sleep(5000L);
            
            // 数据输出完毕, watermark会被调整为long最大值

        }

        @Override
        public void cancel() {

        }
    }
}
