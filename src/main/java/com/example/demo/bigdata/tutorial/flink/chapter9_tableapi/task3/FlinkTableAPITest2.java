package com.example.demo.bigdata.tutorial.flink.chapter9_tableapi.task3;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/25 19:50
 * @Version: 1.0
 */
public class FlinkTableAPITest2 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String inputTblDDL = "CREATE TABLE input_tbl (" +
                " name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                ") WITH ( " +
                "'connector' = 'filesystem', " +
                "'path' = 'D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\input\\click.txt', " +
                "'format' = 'csv') ";
        tableEnv.executeSql(inputTblDDL);
        streamTableEnv.executeSql(inputTblDDL);

        String groupedTbl = "CREATE TABLE output_grp (" +
                " name STRING, " +
                " `count` BIGINT " +
                ") WITH ( " +
                "'connector' = 'print'" +
                ")";
        tableEnv.executeSql(groupedTbl);

        Table aggregateTbl = tableEnv.sqlQuery("select name, count(url) from input_tbl group by name");
        aggregateTbl.executeInsert("output_grp");
    }
}
