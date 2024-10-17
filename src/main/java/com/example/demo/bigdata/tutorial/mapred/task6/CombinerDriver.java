package com.example.demo.bigdata.tutorial.mapred.task6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 21:30
 * @description combiner应用
 */
public class CombinerDriver {
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CombinerDriver.class);
        job.setMapperClass(CombinerMapper.class);
        job.setReducerClass(CombinerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置combiner
        job.setCombinerClass(WcCombiner.class);
//        job.setCombinerClass(CombinerReducer.class);

//        // 无reducer就无shuffle阶段
//        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\input\\task6"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\output\\task6"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
