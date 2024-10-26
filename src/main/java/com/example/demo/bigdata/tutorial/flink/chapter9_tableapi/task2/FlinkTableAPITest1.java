package com.example.demo.bigdata.tutorial.flink.chapter9_tableapi.task2;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description: TableEnvironment
 * @Author: Chenyang on 2024/10/25 15:54
 * @Version: 1.0
 */
public class FlinkTableAPITest1 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                // 流处理
                .inStreamingMode()
//                .inBatchMode()
                // blink planner
                .useBlinkPlanner()
//                .useOldPlanner()
                .build();

        // 创建TableEnvironment
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 输入表
        String inputSql = "CREATE TABLE input_tbl (" +
                "name STRING, " +
                "url STRING, " +
                "`timestamp` BIGINT " +
                ") WITH ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\input\\click.txt', " +
                " 'format' = 'csv' " +
                ")";
        tableEnv.executeSql(inputSql);

        // 文件输出表
        String fsOutputSql = "CREATE TABLE output_fs (" +
                "name STRING, " +
                "url STRING " +
                ") WITH ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\output', " +
                " 'format' = 'csv' " +
                ")";
        tableEnv.executeSql(fsOutputSql);

        // 控制台输出表
        String consoleOutputSql = "CREATE TABLE output_cs(" +
                "name STRING, " +
                "url STRING " +
                ") WITH ( " +
                "'connector' = 'print'" +
                ")";
        tableEnv.executeSql(consoleOutputSql);

        // tableAPI查询转换
        Table inputTable = tableEnv.from("input_tbl");
        Table tbl_jeremy = inputTable.select($("name"), $("url"), $("timestamp"))
                .where($("name").isEqual("Jeremy"));
        // 注册表到环境
        tableEnv.createTemporaryView("tbl_jeremy", tbl_jeremy);

        // sql查询转换
        Table resultTable = tableEnv.sqlQuery("select name, url from tbl_jeremy");

        // 输出表
//        resultTable.executeInsert("output_fs");
        resultTable.executeInsert("output_cs");

    }
}
