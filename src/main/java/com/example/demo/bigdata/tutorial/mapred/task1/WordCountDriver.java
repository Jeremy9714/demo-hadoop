package com.example.demo.bigdata.tutorial.mapred.task1;

import org.apache.commons.lang3.StringUtils;
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
 * @create 2024-10-16 10:15
 * @description MR测试
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String inputPath = args[0];
        String outputPath = args[1];
        if (StringUtils.isBlank(inputPath) || StringUtils.isBlank(outputPath)) {
            throw new RuntimeException("路径参数不完整！");
        }

        // 1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        // 3.关联mapper/reducer相关类
        job.setMapperClass(WordCountMapper.class);

        // 4.设置map输出的kv类型
        job.setReducerClass(WordCountReducer.class);

        // 5.设置最终输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 6.设置输入、输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 7.提交作业
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
