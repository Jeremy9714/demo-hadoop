package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task12;

import com.example.demo.bigdata.tutorial.flink.common.EventGenerator;
import com.example.demo.bigdata.tutorial.flink.common.FlinkEvent;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description: sink mysql
 * @Author: Chenyang on 2024/10/21 19:24
 * @Version: 1.0
 */
public class FlinkSinkTest5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<FlinkEvent> dataStream = env.fromCollection(EventGenerator.getFlinkEventList());

        // 配置JdbcSink
        dataStream.addSink(JdbcSink.sink("insert into from_flink values(?,?,?)",
                ((preparedStatement, flinkEvent) -> {
                    preparedStatement.setString(1, flinkEvent.getId());
                    preparedStatement.setString(2, flinkEvent.getName());
                    preparedStatement.setInt(3, flinkEvent.getAge());
                }),
                // 配置连接信息
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/db_master?useUnicode=true&useSSL=false&characterEncoding=utf-8")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("9714")
                        .build()));

        System.out.println("=====任务提交=====");
        env.execute();
    }
}
