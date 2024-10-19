package com.example.demo.bigdata.tutorial.hadoop.mapred.task4;

import com.example.demo.bigdata.tutorial.hadoop.mapred.task2.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 18:15
 * @description 自定义partitioner分区
 */
public class CustomPartitionerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CustomPartitionerDriver.class);
        job.setMapperClass(CustomPartitionerMapper.class);
        job.setReducerClass(CustomPartitionerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置分区partitioner
        job.setPartitionerClass(ProvincePartitioner.class);
        // 设置reduceTask数量
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\input\\task4"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\output\\task4"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
