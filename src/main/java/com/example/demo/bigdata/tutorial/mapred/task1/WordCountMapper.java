package com.example.demo.bigdata.tutorial.mapred.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 10:22
 * @description mapper测试
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text keyOut = new Text();
    private IntWritable valueOut = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取每行数据
        String line = value.toString();
        // 切割
        String[] words = line.split(" ");
        // 写出
        for (String word : words) {
            keyOut.set(word);
            context.write(keyOut, valueOut);
        }
    }
}
