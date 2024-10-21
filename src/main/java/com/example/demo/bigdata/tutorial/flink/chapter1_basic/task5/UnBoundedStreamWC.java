package com.example.demo.bigdata.tutorial.flink.chapter1_basic.task5;

import com.example.demo.bigdata.common.util.CTools;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description: 无界流处理
 * @Author: Chenyang on 2024/10/19 21:47
 * @Version: 1.0
 */
public class UnBoundedStreamWC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取启动参数
//        MultipleParameterTool multipleParameterTool = MultipleParameterTool.fromArgs(args);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port;
        if (CTools.isBlank(host)) {
            host = "hadoop202";
            port = 12345;
        } else {
            port = parameterTool.getInt("port");
        }
        DataStreamSource<String> inputStream = env.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = inputStream.flatMap((String value, Collector<Tuple2<String, Long>> out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(key -> key.f0)
                .sum(1);

        resultStream.print();

        System.out.println("======服务开启======");
        env.execute();
    }
}
