package com.example.demo.bigdata.tutorial.hadoop.mapred.task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 15:35
 * @description CombineTextInputFormat切片(处理小文件)
 */
public class WordCountCombineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountCombineDriver.class);
        job.setMapperClass(WordCountCombineMapper.class);
        job.setReducerClass(WordCountCombineReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置InputFormat类型, 默认为TextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 设置虚拟存储切片最大值
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304); // 4M
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520); // 20M

        FileInputFormat.setInputPaths(job, new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\input\\task3"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\output\\task3"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
