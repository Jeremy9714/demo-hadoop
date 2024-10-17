package com.example.demo.bigdata.tutorial.mapred.task5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 20:08
 * @description
 */
public class ComparableMapper extends Mapper<LongWritable, Text, ComparableFlowBean, Text> {

    private ComparableFlowBean keyOut = new ComparableFlowBean();
    private Text valueOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");

        String upFlow = words[1];
        String downFlow = words[2];
        keyOut.setUpFlow(Long.parseLong(upFlow));
        keyOut.setDownFlow(Long.parseLong(downFlow));
        keyOut.setSumFlow();

        valueOut.set(words[0]);

        context.write(keyOut, valueOut);

    }
}
