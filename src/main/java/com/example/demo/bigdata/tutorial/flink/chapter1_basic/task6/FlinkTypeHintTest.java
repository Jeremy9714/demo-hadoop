package com.example.demo.bigdata.tutorial.flink.chapter1_basic.task6;

import com.example.demo.bigdata.tutorial.flink.common.SourceEvent1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @Description: type hint
 * @Author: Chenyang on 2024/10/21 10:16
 * @Version: 1.0
 */
public class FlinkTypeHintTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> dataStream = env.readTextFile("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\input\\detail.txt");
        SingleOutputStreamOperator<SourceEvent1> resultStream = dataStream.flatMap((String line, Collector<SourceEvent1> out) -> {
            String[] words = line.split(" ");
            SourceEvent1 event = new SourceEvent1();
            event.setId(Integer.parseInt(words[0]));
            event.setName(words[1]);
            event.setTimestamp(Long.parseLong(words[2]));
            out.collect(event);
        })
                .returns(SourceEvent1.class);
//                .returns(new TypeHint<SourceEvent1>() {});
//                .returns(Types.POJO(SourceEvent1.class, new HashMap<String, TypeInformation<?>>() {{
//                    put("id", TypeInformation.of(Integer.class));
//                    put("name", TypeInformation.of(String.class));
//                    put("timestamp", Types.LONG);
//                }}));

        resultStream.print();

        System.out.println("=====任务提交=====");
        env.execute();
    }
}
