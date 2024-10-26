package com.example.demo.bigdata.tutorial.flink.chapter9_tableapi.task7;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description: tableAPI窗口topN
 * @Author: Chenyang on 2024/10/26 12:46
 * @Version: 1.0
 */
public class TopNTableAPITest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createDDL = "create table input_tbl (" +
                "name string, " +
                "url string, " +
                "ts bigint, " +
                "et as to_timestamp(from_unixtime(ts/1000)), " +
                "watermark for et as et - interval '0' second" +
                ") with ( " +
                "'connector' = 'filesystem', " +
                "'path' = 'D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\input\\click.txt', " +
                "'format' = 'csv' " +
                ")";
        tableEnv.executeSql(createDDL);

        String groupSql = "select name, count(1) as cnt, window_start, window_end " +
                "from table( " +
                "   tumble( table input_tbl, descriptor(et), interval '10' second)" +
                ") " +
                "group by name, window_start, window_end";

        String querySql = "select name, cnt, row_num, window_start, window_end " +
                "from ( " +
                "   select *, row_number() over( " +
                "       partition by window_start, window_end " +
                "       order by cnt desc " +
                "   ) as row_num " +
                "   from ( " +
                groupSql +
                "   )" +
                ") " +
                "where row_num <= 2";

        Table table = tableEnv.sqlQuery(querySql);
        tableEnv.toChangelogStream(table).print();

        env.execute();
    }
}
