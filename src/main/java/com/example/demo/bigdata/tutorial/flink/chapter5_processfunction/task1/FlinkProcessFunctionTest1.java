package com.example.demo.bigdata.tutorial.flink.chapter5_processfunction.task1;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description: ProcessFunction
 * @Author: Chenyang on 2024/10/23 16:50
 * @Version: 1.0
 */
public class FlinkProcessFunctionTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(EventUtils.getSourceFunction2())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SourceEvent2>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<SourceEvent2>) (element, recordTimestamp) -> element.getTimestamp()))
                .process(new ProcessFunction<SourceEvent2, String>() {
                    @Override
                    public void processElement(SourceEvent2 value, Context ctx, Collector<String> out) throws Exception {
                        if ("Jeremy".equals(value.getName())) {
                            out.collect(value.getName() + " clicks " + value.getUrl());
                        }
                        out.collect(value.toString());

                        System.out.println("timestamp: " + ctx.timestamp());
                        System.out.println("processingTime: " + ctx.timerService().currentProcessingTime());
                        System.out.println("watermark: " + ctx.timerService().currentWatermark());
                        System.out.println("subTaskNum: " + getRuntimeContext().getIndexOfThisSubtask());
                        System.out.println("================");
                    }
                })
                .print();

        env.execute();
    }
}
