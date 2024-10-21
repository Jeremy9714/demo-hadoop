package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task7;

import com.example.demo.bigdata.tutorial.flink.common.EventGenerator;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent1;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @Description: 物理分区
 * @Author: Chenyang on 2024/10/21 16:19
 * @Version: 1.0
 */
public class FlinkTransformationTest5 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<SourceEvent2> dataStream1 = env.fromCollection(EventGenerator.getEvent2List());
        DataStreamSource<SourceEvent1> dataStream1 = env.fromCollection(EventGenerator.getEvent1List());

        // 随机分区
//        dataStream1.shuffle().print().setParallelism(4);

        // 轮询分区 
//        dataStream1.rebalance().print().setParallelism(4);

        // 重缩放分区
//        env.addSource(new MyRichParallelSourceFunction())
//                .setParallelism(2)
//                .rescale()
//                .print()
//                .setParallelism(4);

        // 广播分区
//        env.fromCollection(EventGenerator.getEvent2List())
//                .broadcast()
//                .print()
//                .setParallelism(4);

        // 全局分区
//        dataStream1.global().print().setParallelism(4);

        // 自定义分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom((Partitioner<Integer>) (key, numPartitions) -> key % 2,
                        (KeySelector<Integer, Integer>) value -> value)
                .print().setParallelism(4);


        System.out.println("=====任务提交=====");
        env.execute();
    }

    public static class MyRichParallelSourceFunction extends RichParallelSourceFunction<Integer> {

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            for (int i = 1; i < 9; ++i) {
                if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                    ctx.collect(i);
                }
            }
        }

        @Override
        public void cancel() {

        }
    }
}
