package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task8;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @Description: sink
 * @Author: Chenyang on 2024/10/21 16:42
 * @Version: 1.0
 */
public class FlinkSinkTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<SourceEvent2> inputStream = env.fromCollection(EventUtils.getEvent2List());

        // fileSink
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\output\\output"),
                new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        // 滚动策略
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024) // 文件大小
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) // 滚动周期
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) // 超时周期
                                .build()
                )
                .build();

        // 添加sink
        inputStream.map(SourceEvent2::toString)
                .addSink(streamingFileSink);

        System.out.println("=====任务提交=====");
        env.execute();
    }
}
