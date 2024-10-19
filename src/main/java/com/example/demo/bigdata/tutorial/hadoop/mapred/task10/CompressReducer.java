package com.example.demo.bigdata.tutorial.hadoop.mapred.task10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-17 12:46
 * @description
 */
public class CompressReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable valueOut = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        valueOut.set(sum);
        context.write(key, valueOut);
    }
}
