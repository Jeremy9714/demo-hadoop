package com.example.demo.bigdata.tutorial.hadoop.mapred.task8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-17 11:03
 * @description
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String fileName;
    private Text keyOut = new Text();
    private TableBean valueOut = new TableBean();

    @Override
    protected void setup(Context context) {
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");

        if (fileName.contains("order")) {
            keyOut.set(words[1]);
            valueOut.setId(words[0]);
            valueOut.setPId(words[1]);
            valueOut.setAmount(Integer.parseInt(words[2]));
            valueOut.setPName("");
            valueOut.setFlag("order");
        } else {
            keyOut.set(words[0]);
            valueOut.setId("");
            valueOut.setPId(words[0]);
            valueOut.setAmount(0);
            valueOut.setPName(words[1]);
            valueOut.setFlag("pd");
        }
        context.write(keyOut, valueOut);
    }
}
