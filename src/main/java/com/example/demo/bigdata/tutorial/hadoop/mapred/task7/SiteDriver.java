package com.example.demo.bigdata.tutorial.hadoop.mapred.task7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 21:58
 * @description 自定义outputFormat
 */
public class SiteDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SiteDriver.class);
        job.setMapperClass(SiteMapper.class);
        job.setReducerClass(SiteReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置OutputFormat
        job.setOutputFormatClass(SiteOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\input\\task7"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\output\\task7"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
