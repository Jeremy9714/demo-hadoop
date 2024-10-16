package com.example.demo.bigdata.tutorial.mapred.task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 13:40
 * @description
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text keyOut = new Text();
    private FlowBean valueOut = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] vars = line.split("\t");

        keyOut.set(vars[1]);
        valueOut.setUpFlow(Long.parseLong(vars[vars.length - 3]));
        valueOut.setDownFlow(Long.parseLong(vars[vars.length - 2]));
        valueOut.setSumFlow();

        context.write(keyOut, valueOut);
    }
}
