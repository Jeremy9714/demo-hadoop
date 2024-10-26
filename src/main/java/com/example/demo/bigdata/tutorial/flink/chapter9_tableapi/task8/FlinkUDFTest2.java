package com.example.demo.bigdata.tutorial.flink.chapter9_tableapi.task8;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/26 16:00
 * @Version: 1.0
 */
public class FlinkUDFTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createDDL = "create table input_tbl ( " +
                "name string, " +
                "url string, " +
                "ts bigint, " +
                "et as to_timestamp(from_unixtime(ts/1000)), " +
                "watermark for et as et - interval '0' second " +
                ") with ( " +
                "'connector' = 'filesystem', " +
                "'path' = 'D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\input\\click.txt', " +
                "'format' = 'csv' " +
                ")";
        tableEnv.executeSql(createDDL);

        String windowSql = "select name, count(1) as cnt, window_start, window_end " +
                "from table( " +
                "tumble( table input_tbl, descriptor(et), interval '10' second )" +
                ") group by name, window_start, window_end";

        // 表聚合函数TableAggregateFunction
        tableEnv.createTemporarySystemFunction("top2", MyTableAggregateFunction.class);
        Table table = tableEnv.sqlQuery(windowSql);
        Table resultTable = table.groupBy($("window_end"))
//        Table resultTable = table.groupBy($("name"), $("window_end"), $("window_start"))
                .flatAggregate(call("top2", $("cnt")).as("value", "rank"))
//                .select($("name"), $("window_start"), $("window_end"), $("value"), $("rank"));
                .select($("window_end"), $("value"), $("rank"));

        tableEnv.toChangelogStream(resultTable).print();

        env.execute();
    }

    // 第一个泛型是返回类型 第二个泛型是累加器泛型
    public static class MyTableAggregateFunction extends TableAggregateFunction<Tuple2<Long, Integer>, List<Long>> {
        @Override
        public List<Long> createAccumulator() {
            return new ArrayList<>();
        }

        // 第一个参数为累加器, 后面的参数为入参(可以多个)
        public void accumulate(List<Long> acc, Long cnt) {
            acc.add(cnt);
        }

        // 第一个参数为累加器, 第二个参数为收集器
        public void emitValue(List<Long> acc, Collector<Tuple2<Long, Integer>> out) {
            acc.sort((l1, l2) -> Long.compare(l2, l1));
            for (int i = 0; i < Math.min(2, acc.size()); ++i) {
                out.collect(Tuple2.of(acc.get(i), (i + 1)));
            }
        }
    }
}
