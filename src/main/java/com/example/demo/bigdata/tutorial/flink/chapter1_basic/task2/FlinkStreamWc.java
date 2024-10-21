package com.example.demo.bigdata.tutorial.flink.chapter1_basic.task2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description: flink 1.10.1 批处理
 * @Author: Chenyang on 2024/10/19 15:50
 * @Version: 1.0
 */
public class FlinkStreamWc {

    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8); // 并行度，默认核数

        // 从文件读取数据
        String inputPath = "D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\input\\helloworld.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);

//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        int port = 0;
//        String host = parameterTool.get("host");
//        if (StringUtils.isBlank(host)) {
//            host = "k8s-master";
//            port = 10000;
//        } else {
//            port = parameterTool.getInt("port");
//        }
//
//        // 从socket读取数据
//        DataStream<String> inputStream = env.socketTextStream(host, port);

        DataStream<Tuple2<String, Integer>> resultStream = inputStream
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                // 指定泛型
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1);

        resultStream.print();

        System.out.println("==========任务启动==========");
        // 启动任务
        env.execute();

    }
}
