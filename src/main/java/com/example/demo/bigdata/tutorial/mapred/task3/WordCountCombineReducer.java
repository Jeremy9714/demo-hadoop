package com.example.demo.bigdata.tutorial.mapred.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 15:32
 * @description
 */
public class WordCountCombineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
