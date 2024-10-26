package com.example.demo.bigdata.tutorial.flink.chapter9_tableapi.task6;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description: 窗口
 * @Author: Chenyang on 2024/10/26 9:28
 * @Version: 1.0
 */
public class FlinkTableAPITest5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // DDL中定时时间语义属性
        SingleOutputStreamOperator<SourceEvent2> dataStream = env.addSource(EventUtils.getSourceFunction2())
                .assignTimestampsAndWatermarks(EventUtils.getMyPeriodicWatermarkStrategy(0L));
        tableEnv.createTemporaryView("input_tbl", dataStream, $("name"), $("url"), $("timestamp").as("ts"), $("et").rowtime());

        // 分组窗口聚合
        Table aggTable = tableEnv.sqlQuery("select name, count(1) from input_tbl group by name");
//        tableEnv.toChangelogStream(aggTable).print("agg");

        // 滚动窗口
        Table tumblingTable = tableEnv.sqlQuery("select name, count(1) as cnt, " +
                "window_end as endT " +
                "from TABLE( " +
                "TUMBLE(TABLE input_tbl, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY name, window_end, window_start"
        );

        // 滑动窗口
        Table slidingTable = tableEnv.sqlQuery("select name, count(1) as cnt, " +
                " window_end as endT " +
                "from TABLE(" +
                "HOP(TABLE input_tbl, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") " +
                "group by name, window_end, window_start"
        );

        // 累计窗口
        Table cumulateTable = tableEnv.sqlQuery("select name, count(1) as cnt, " +
                "window_end as endT " +
                "from table( " +
                "cumulate(table input_tbl, descriptor(et), interval '5' second, interval '10' second)" +
                ") " +
                "group by name, window_end, window_start"
        );

        // 开窗聚合
        // 数据范围
        Table rangeOverTable = tableEnv.sqlQuery("select name, " +
                "avg(ts) over (" + // 聚合函数
                " partition by name " + // 分区
                " order by et " + // 时间升序
                " range between interval '5' second preceding and current row " + // 前5秒时间范围
                ") as avg_ts " +
                "from input_tbl"
        );

        // 时间范围
        Table rowOverTable = tableEnv.sqlQuery("select name, " +
                "avg(ts) over (" +
                " partition by name " +
                " order by et " +
                " rows between 5 preceding and current row " + // 前5行数据范围
                ") as avg_ts " +
                "from input_tbl"
        );


//        tableEnv.toDataStream(tumblingTable).print("tumbling");
//        tableEnv.toDataStream(slidingTable).print("sliding");
//        tableEnv.toDataStream(cumulateTable).print("cumulate");
        tableEnv.toChangelogStream(rangeOverTable).print("rangeOver");
        tableEnv.toChangelogStream(rowOverTable).print("rowOver");

        env.execute();
    }
}
