package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task10;

import com.example.demo.bigdata.tutorial.flink.common.EventGenerator;
import com.example.demo.bigdata.tutorial.flink.common.FlinkEvent;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Description: sink redis
 * @Author: Chenyang on 2024/10/21 17:27
 * @Version: 1.0
 */
public class FlinkSinkTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<FlinkEvent> dataStream = env.fromCollection(EventGenerator.getFlinkEventList());

        // 建立jedis连接
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop204")
                .setPort(6379)
                .setPassword("9714")
                .build();

        // 写入redis
        dataStream.addSink(new RedisSink<>(redisConfig, new MyRedisMapper()));

        System.out.println("=====任务提交=====");
        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<FlinkEvent> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"from-flink");
        }

        @Override
        public String getKeyFromData(FlinkEvent data) {
            return data.getName();
        }

        @Override
        public String getValueFromData(FlinkEvent data) {
            return data.toString();
        }
    }
}
