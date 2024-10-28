package com.example.demo.bigdata.tutorial.flink.chapter10_cep.task1;

import com.example.demo.bigdata.tutorial.flink.common.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Description: cep简单使用
 * @Author: Chenyang on 2024/10/26 20:52
 * @Version: 1.0
 */
public class FlinkCepTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<LoginEvent> dataStream = env.fromElements(
                new LoginEvent("Jeremy", "192.168.85.100", "fail", 2000L),
                new LoginEvent("Jeremy", "192.168.85.101", "fail", 3000L),
                new LoginEvent("Sean", "192.168.85.102", "fail", 4000L),
                new LoginEvent("Jeremy", "192.168.85.100", "fail", 5000L),
                new LoginEvent("Sean", "192.168.85.102", "fail", 9000L),
                new LoginEvent("Sean", "192.168.85.102", "fail", 11000L),
                new LoginEvent("Sean", "192.168.85.100", "success", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (element, recordTimestamp) -> element.getTimestamp()));

        // 定义匹配规则
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first") // 事件名
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equalsIgnoreCase(value.getEventType());
                    }
                })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equalsIgnoreCase(value.getEventType());
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equalsIgnoreCase(value.getEventType());
                    }
                });

        // 将匹配规则应用到事件流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(dataStream.keyBy(LoginEvent::getUserId), pattern);

        // 对检测到的复杂事件进行处理
        SingleOutputStreamOperator<String> warningStream = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            // map保存定义规则匹配到的事件
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent firstEvent = map.get("first").get(0);
                LoginEvent secondEvent = map.get("second").get(0);
                LoginEvent thirdEvent = map.get("third").get(0);
                return firstEvent.getUserId() + "连续登录失败三次! 登录时间为: " +
                        firstEvent.getTimestamp() + ", " +
                        secondEvent.getTimestamp() + ", " +
                        thirdEvent.getTimestamp();
            }
        });

        warningStream.print();

        env.execute();
    }
}
