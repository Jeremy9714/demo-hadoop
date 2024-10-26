package com.example.demo.bigdata.tutorial.flink.chapter9_tableapi.task8;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Description: UDF
 * @Author: Chenyang on 2024/10/26 13:50
 * @Version: 1.0
 */
public class FlinkUDFTest1 {
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

        // 标量函数ScalarFunction
        // 注册udf
        tableEnv.createTemporarySystemFunction("myHash", MyScalarFunction.class);

//        // tableAPI调用udf
//        Table tableAPIResult = tableEnv.from("input_tbl").select($("name"), call("myHash", $("name")));
//        tableEnv.toDataStream(tableAPIResult).print("tableAPI");

        // sql调用udf
        Table scalarTable = tableEnv.sqlQuery("select name, myHash(name) from input_tbl");
//        tableEnv.toDataStream(scalarTable).print("scalarSql");

        // 表函数TableFunction
        tableEnv.createTemporarySystemFunction("MySplit", MyTableFunction.class);
        // lateral table 侧向表
        Table tblTable = tableEnv.sqlQuery("select name, url, sub_url, length from " +
                "input_tbl, lateral table(mySplit(url))");
//                "input_tbl, lateral table(mySplit(url)) as T(sub_url,length)"); // 重命名侧向表中字段
//                "input_tbl, left join lateral table(mySplit(url)) as T(sub_url,length) on true");
//        tableEnv.toDataStream(tblTable).print("tableSql");

        // 聚合函数AggregateFunction
        tableEnv.createTemporarySystemFunction("myAgg", MyAggregateFunction.class);
        Table aggTable = tableEnv.sqlQuery("select name, myAgg(ts,1) as ts_wAvg from input_tbl group by name");
//        tableEnv.toChangelogStream(aggTable).print("aggSql");

        env.execute();
    }

    // 自定义标量方法
    public static class MyScalarFunction extends ScalarFunction {

        // 固定名称eval
        public int eval(String value) {
            return value.hashCode();
        }
    }

    // 自定义表方法
    // 字段名称类型提示(ROW)
    @FunctionHint(output = @DataTypeHint("ROW<sub_url STRING, length INT>"))
    public static class MyTableFunction extends TableFunction<Row> {

        public void eval(String str) {
            String[] split = str.split("\\?");
            for (String s : split) {
                collect(Row.of(s, s.length()));
            }
        }
    }

//    public static class MyTableFunction extends TableFunction<Tuple2<String, Integer>> {
//
//        // 固定名称eval
//        public void eval(String value) {
//            String[] split = value.split("\\?");
//            for (String s : split) {
//                collect(Tuple2.of(s, s.length()));
//            }
//        }
//    }

    // 自定义聚合方法
    public static class MyAggregateFunction extends AggregateFunction<Long, Tuple2<Long, Long>> {

        // 获取结果
        @Override
        public Long getValue(Tuple2<Long, Long> acc) {
            if (acc.f1 == 0) {
                return null;
            } else {
                return acc.f0 / acc.f1;
            }
        }

        // 创建累加器
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(0L, 0L);
        }

        // 固定方法accumulate
        public void accumulate(Tuple2<Long, Long> acc, Long iValue, Long iWeight) {
            acc.f0 += iValue * iWeight;
            acc.f1 += iWeight;
        }
    }
}
