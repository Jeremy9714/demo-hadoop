package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Description: 自定义source算子
 * @Author: Chenyang on 2024/10/21 10:01
 * @Version: 1.0
 */
public class FlinkCustomSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 单并发度
        DataStream<String> serialStream = env.addSource(new MySerialSourceFunction());
        serialStream.print("serial");

        // 多并发度
        DataStream<Integer> parallelStream = env.addSource(new MyParallelSourceFunction()).setParallelism(2);
        parallelStream.print("parallel");

        System.out.println("=====任务提交=====");
        env.execute();
    }

    public static class MySerialSourceFunction implements SourceFunction<String> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                ctx.collect(random.nextInt(100) + "");
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class MyParallelSourceFunction implements ParallelSourceFunction<Integer> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(random.nextInt());
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
