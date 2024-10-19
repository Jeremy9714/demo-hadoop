package com.example.demo.bigdata.tutorial.hadoop.mapred.task5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 20:17
 * @description
 */
public class ComparableReducer extends Reducer<ComparableFlowBean, Text, Text, ComparableFlowBean> {

    @Override
    protected void reduce(ComparableFlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
