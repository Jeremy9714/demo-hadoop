package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task6;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description: UDF 富函数
 * @Author: Chenyang on 2024/10/21 12:18
 * @Version: 1.0
 */
public class FlinkTransformationTest4 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SourceEvent2> dataStream = env.fromCollection(EventUtils.getEvent2List());
        dataStream.map(new MyRichMapper()).print().setParallelism(2);

        env.execute();
    }

    public static class MyRichMapper extends RichMapFunction<SourceEvent2, String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("=====open被调用: " + getRuntimeContext().getIndexOfThisSubtask() + "======");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("======close被调用======");
        }

        @Override
        public String map(SourceEvent2 value) throws Exception {
            return value.toString();
        }
    }

}
