package com.example.demo.bigdata.tutorial.flink.chapter9_tableapi.task7;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description: tableAPI实现topN问题
 * @Author: Chenyang on 2024/10/26 11:30
 * @Version: 1.0
 */
public class TopNTableAPITest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createDDL = "create table input_tbl (" +
                "name STRING, " +
                "url STRING, " +
                "ts BIGINT, " +
                "et as to_timestamp(from_unixtime(ts / 1000)), " +
                "watermark for et as et - interval '0' second" +
                ") with ( " +
                "'connector' = 'filesystem', " +
                "'path' = 'D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\input\\click.txt', " +
                "'format' = 'csv'" +
                ")";

        tableEnv.executeSql(createDDL);

        // TopN
        Table table = tableEnv.sqlQuery("select name, cnt, row_num " +
                "from ( " +
                "  select *, ROW_NUMBER() over( " + // 添加行号
                "       order by cnt desc" + // 数量降序
                "    ) as row_num " +
                "  from ( select name, count(1) as cnt from input_tbl group by name) " + // 分组计算数量
                ") where row_num <= 2"
        );

        tableEnv.toChangelogStream(table).print();

        env.execute();
    }
}
