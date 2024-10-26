package com.example.demo.bigdata.tutorial.flink.chapter9_tableapi.task4;

import com.example.demo.bigdata.tutorial.flink.common.EventUtils;
import com.example.demo.bigdata.tutorial.flink.common.SourceEvent2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/25 21:49
 * @Version: 1.0
 */
public class FlinkTableAPITest3 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<SourceEvent2> dataStream = env.addSource(EventUtils.getSourceFunction2())
                .assignTimestampsAndWatermarks(EventUtils.getMyPeriodicWatermarkStrategy(0L));
        DataStreamSource<Long> longStream = env.addSource(EventUtils.getLongSource());

        // Row类型的更新数据流
        DataStream<Row> rowStream = env.fromElements(
                Row.of(RowKind.INSERT, "Jeremy", 27),
                Row.of(RowKind.INSERT, "Mary", 18),
                Row.of(RowKind.UPDATE_BEFORE, "Sean", 18),
                Row.of(RowKind.UPDATE_AFTER, "Sean", 20)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        Table inputTable = tableEnv.fromDataStream(dataStream, $("name"), $("url"), $("timestamp").as("ts"));

        // dataStream注册为table
        tableEnv.createTemporaryView("input_tbl", dataStream, $("name"), $("url"), $("timestamp"));
        // 默认字段名为f0 (一元组)
        tableEnv.createTemporaryView("input_tbl_long", longStream, $("my_long"));
        // 更新日志流转换
        Table rowTbl = tableEnv.fromChangelogStream(rowStream);
        

    }
}
