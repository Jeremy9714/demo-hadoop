package com.example.demo.bigdata.tutorial.hadoop.mapred.task4;

import com.example.demo.bigdata.tutorial.hadoop.mapred.task2.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 18:15
 * @description
 */
public class CustomPartitionerMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text keyOut = new Text();
    private FlowBean valueOut = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        String phoneNum = words[1];
        String upFlow = words[words.length - 3];
        String downFlow = words[words.length - 2];

        keyOut.set(phoneNum);
        valueOut.setUpFlow(Long.parseLong(upFlow));
        valueOut.setDownFlow(Long.parseLong(downFlow));
        valueOut.setSumFlow();

        context.write(keyOut, valueOut);
    }
}
