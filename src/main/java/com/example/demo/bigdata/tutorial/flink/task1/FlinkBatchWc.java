package com.example.demo.bigdata.tutorial.flink.task1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description: flink 1.10.1 批处理
 * @Author: Chenyang on 2024/10/19 11:03
 * @Version: 1.0
 */
public class FlinkBatchWc {

    private static Logger log = LoggerFactory.getLogger(FlinkBatchWc.class);

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从取数据
        String inputPath = "D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\flink\\input\\task1\\helloworld.txt";
        DataSet<String> inputDs = env.readTextFile(inputPath);

        // 分词，并统计
        DataSet<Tuple2<String, Integer>> resultSet = inputDs.flatMap(new MyFlatMapper())
                .groupBy(0) // 按第一个位置数据分组
                .sum(1);// 将第二位置的数据求和

        System.out.println("===========输出结果==========");
        log.info("debug模式");
        resultSet.print();

    }

    // 自定义FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 按空格分词
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
