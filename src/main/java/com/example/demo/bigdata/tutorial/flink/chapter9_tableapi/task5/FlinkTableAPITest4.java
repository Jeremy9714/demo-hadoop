package com.example.demo.bigdata.tutorial.flink.chapter9_tableapi.task5;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description: 时间语义
 * @Author: Chenyang on 2024/10/26 8:34
 * @Version: 1.0
 */
public class FlinkTableAPITest4 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 自动使用最后一个long字段作为水位线
        SingleOutputStreamOperator<SourceEvent2> dataStream = env.addSource(EventUtils.getSourceFunction2())
                .assignTimestampsAndWatermarks(EventUtils.getMyPeriodicWatermarkStrategy(0L));
        Table table = tableEnv.fromDataStream(dataStream, $("name"), $("url"), /*$("ts").proctime(),*/ $("ts").rowtime());
        table.printSchema();
        tableEnv.createTemporaryView("tbl_input", table);

        // 自定义提取时间戳并生成水位线
        Table table1 = tableEnv.fromDataStream(dataStream, $("name"), $("url"), $("timestamp"), $("et").rowtime());
        tableEnv.createTemporaryView("tbl_input1", table1);
        table1.printSchema();

        // 直接定义水位线 (AS 计算列)
        String createDDLWithWatermark1 = "CREATE TABLE tbl_input2 (" +
                "name STRING, " +
                "url STRING, " +
                "ts BIGINT, " +
                "et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000)), " +
                "WATERMARK FOR et AS et - INTERVAL '2' SECOND " +
                ") WITH ( " +
                "'connector' = 'filesystem', " +
                "'path' = 'D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\input\\click.txt', " +
                "'format' = 'csv'" +
                ")";
        tableEnv.executeSql(createDDLWithWatermark1);
        Table table2 = tableEnv.sqlQuery("select * from tbl_input2");
        table2.printSchema();


        // 直接定义水位线
        String createDDLWithWatermark2 = "CREATE TABLE tbl_input3 (" +
                "name STRING, " +
                "url STRING, " +
                "ts TIMESTAMP(3), " +
                "pt as PROCTIME()" +
                "WATERMARK FOR ts AS ts - INTERVAL '2' SECOND " +
                ") WITH ( " +
                "'connector' = 'filesystem', " +
                "'path' = 'D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\input\\click.txt', " +
                "'format' = 'csv'" +
                ")";
        tableEnv.executeSql(createDDLWithWatermark2);
        Table table3 = tableEnv.sqlQuery("select * from tbl_input3");
        table3.printSchema();
    }
}
