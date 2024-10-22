package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task3;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent1;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description: transformation算子
 * @Author: Chenyang on 2024/10/21 10:38
 * @Version: 1.0
 */
public class FlinkTransformationTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStream<SourceEvent1> dataStream = env.fromElements(new SourceEvent1(1, "Jean", 1000L),
//                new SourceEvent1(2, "Joe", 2000L),
//                new SourceEvent1(3, "Sean", 3000L));
        DataStream<SourceEvent1> dataStream = env.fromCollection(EventUtils.getEvent1List());

        // map算子
        DataStream<String> mappedStream = dataStream.map(new MyMapper());
//        dataStream.map(SourceEvent1::getName);
//        dataStream.map(item->item.getName());
        mappedStream.print("map");

        // filter算子
        SingleOutputStreamOperator<SourceEvent1> filteredStream = dataStream.filter(new MyFilter());
        filteredStream.print("filter");

        // flatMap算子
        SingleOutputStreamOperator<String> flatMappedStream = dataStream.flatMap(new myFlatMapper());
        flatMappedStream.print("flatmap");

        System.out.println("=====提交任务=====");
        env.execute();
    }

    public static class MyMapper implements MapFunction<SourceEvent1, String> {
        @Override
        public String map(SourceEvent1 value) throws Exception {
            return value.getName();
        }
    }

    public static class MyFilter implements FilterFunction<SourceEvent1> {
        @Override
        public boolean filter(SourceEvent1 value) throws Exception {
            return value.getTimestamp() > 4000L;
        }
    }

    public static class myFlatMapper implements FlatMapFunction<SourceEvent1, String> {
        @Override
        public void flatMap(SourceEvent1 value, Collector<String> out) throws Exception {
            if (value.getTimestamp() > 4000L) {
                out.collect(Integer.toString(value.getId()));
                out.collect(value.getName());
                out.collect(Long.toString(value.getTimestamp()));
            }
        }
    }
}
