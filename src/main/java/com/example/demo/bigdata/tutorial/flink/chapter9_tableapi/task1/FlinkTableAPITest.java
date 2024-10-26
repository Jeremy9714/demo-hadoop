package com.example.demo.bigdata.tutorial.flink.chapter9_tableapi.task1;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description: TableAPI
 * @Author: Chenyang on 2024/10/25 14:21
 * @Version: 1.0
 */
public class FlinkTableAPITest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SourceEvent2> dataStream = env.addSource(EventUtils.getSourceFunction2())
                .assignTimestampsAndWatermarks(EventUtils.getMyPeriodicWatermarkStrategy(0L));

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将DataStream转换成table
        Table table = tableEnv.fromDataStream(dataStream);

        // 用sql进行转换
        Table resultTable1 = tableEnv.sqlQuery("select name, url, `timestamp` from " + table);
        // 基于Table直接转换
        Table resultTable2 = table.select($("name"), $("url"), $("timestamp"))
                .where($("name").isEqual("Jeremy"));

        // table转换为DataStream
        tableEnv.toDataStream(resultTable1).print("result1");
        tableEnv.toChangelogStream(resultTable2).print("result2");

        tableEnv.createTemporaryView("temp_tbl", table);
        // 聚合转换
        Table streamAggregateTbl = tableEnv.sqlQuery("select name, count(url) from temp_tbl group by name");
        tableEnv.toChangelogStream(streamAggregateTbl).print("agg");

        env.execute();

    }
}
