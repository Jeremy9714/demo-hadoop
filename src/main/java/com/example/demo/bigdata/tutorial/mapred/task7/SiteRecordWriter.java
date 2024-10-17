package com.example.demo.bigdata.tutorial.mapred.task7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 22:04
 * @description
 */
public class SiteRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream standardOutputStream;
    private FSDataOutputStream othersOutputStream;

    public SiteRecordWriter(Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(conf);

            // 创建两条流
            standardOutputStream = fs.create(new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\output\\task7\\standard.log"));
            othersOutputStream = fs.create(new Path("D:\\workplace\\2021-2024\\workplace\\test\\hadoop\\demo-hadoop\\src\\main\\resources\\files\\output\\task7\\others.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String site = key.toString();

        if (site.contains("google")) {
            standardOutputStream.writeBytes(site + "\n");
        } else {
            othersOutputStream.writeBytes(site + "\n");
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(standardOutputStream);
        IOUtils.closeStream(othersOutputStream);
    }
}
