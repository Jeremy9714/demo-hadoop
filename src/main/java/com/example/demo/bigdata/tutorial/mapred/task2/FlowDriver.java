package com.example.demo.bigdata.tutorial.mapred.task2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 13:52
 * @description bean序列化测试
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String inputPath = "D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\input\\task2";
        String outputPath = "D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\output\\task2";

//        String inputPath = args[0];
//        String outputPath = args[1];
//        if (StringUtils.isBlank(inputPath) || StringUtils.isBlank(outputPath)) {
//            throw new RuntimeException("参数不正确！");
//        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
