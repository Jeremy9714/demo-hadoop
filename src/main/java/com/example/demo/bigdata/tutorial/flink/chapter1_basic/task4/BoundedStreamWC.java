package com.example.demo.bigdata.tutorial.flink.chapter1_basic.task4;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description: 有界流处理
 * @Author: Chenyang on 2024/10/19 21:32
 * @Version: 1.0
 */
public class BoundedStreamWC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        
        DataStreamSource<String> dataStream = env.readTextFile("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\input\\helloworld.txt");

        // 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = dataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);
        // 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sumStream = wordAndOneKeyedStream.sum(1);

        sumStream.print();

        System.out.println("======任务提交======");
        // 流处理任务提交
        env.execute();

    }
}
