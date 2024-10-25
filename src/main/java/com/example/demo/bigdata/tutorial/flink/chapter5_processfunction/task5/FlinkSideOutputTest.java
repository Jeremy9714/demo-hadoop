package com.example.demo.bigdata.tutorial.flink.chapter5_processfunction.task5;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Description: sideOutput测试
 * @Author: Chenyang on 2024/10/23 20:46
 * @Version: 1.0
 */
public class FlinkSideOutputTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 输出标签
        OutputTag<Tuple3<String, String, Long>> output1 = new OutputTag<Tuple3<String, String, Long>>("Jeremy") {
        };
        OutputTag<Tuple3<String, String, Long>> output2 = new OutputTag<Tuple3<String, String, Long>>("Sean") {
        };

        SingleOutputStreamOperator<SourceEvent2> dataStream = env.addSource(EventUtils.getSourceFunction2())
                .assignTimestampsAndWatermarks(EventUtils.getMyPeriodicWatermarkStrategy(0L))
                .process(new ProcessFunction<SourceEvent2, SourceEvent2>() {
                    @Override
                    public void processElement(SourceEvent2 value, Context ctx, Collector<SourceEvent2> out) throws Exception {
                        if ("Jeremy".equals(value.getName())) {
                            ctx.output(output1, Tuple3.of(value.getName(), value.getUrl(), value.getTimestamp()));
                        } else if ("Sean".equals(value.getName())) {
                            ctx.output(output2, Tuple3.of(value.getName(), value.getUrl(), value.getTimestamp()));
                        } else {
                            out.collect(value);
                        }
                    }
                });

        dataStream.print("else");
        dataStream.getSideOutput(output1).print("Jeremy");
        dataStream.getSideOutput(output2).print("Sean");

        env.execute();
    }
}
