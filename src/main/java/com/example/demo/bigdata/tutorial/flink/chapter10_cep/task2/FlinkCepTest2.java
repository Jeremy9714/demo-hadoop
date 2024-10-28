package com.example.demo.bigdata.tutorial.flink.chapter10_cep.task2;

import com.example.demo.bigdata.tutorial.flink.common.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Description: cep组合模式
 * @Author: Chenyang on 2024/10/26 22:14
 * @Version: 1.0
 */
public class FlinkCepTest2 {
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


        // 初始条件
        Pattern<LoginEvent, LoginEvent> startPattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        });
        
        // 严格近邻条件
//        startPattern.next().where()

        // 宽松近邻条件
//        startPattern.followedBy("followed").where()

        // 非确定性宽松近邻条件
//        startPattern.followedByAny("followed").where()

        // 不能严格紧邻条件
//        startPattern.notNext("notNext").where()

        // 不能宽松紧邻条件
//        startPattern.notFollowedBy("notFollowed").where().next("next").where()

        // 时间限制条件
//        startPattern.within(Time.minutes(1L));
        
        // 定义了量词的循环模式默认采用宽松机制
//        startPattern.times(3); // 宽松匹配
//        startPattern.times(3).consecutive(); // 严格匹配
//        startPattern.times(3).allowCombinations(); // 非确定性宽松近邻匹配

        env.execute();
    }
}
