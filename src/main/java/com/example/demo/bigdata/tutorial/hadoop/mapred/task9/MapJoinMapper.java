package com.example.demo.bigdata.tutorial.hadoop.mapred.task9;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Chenyang
 * @create 2024-10-17 11:46
 * @description
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pdMap = new HashMap<>();
    private Text keyOut = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        // 打开缓存文件输入流
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        String line;
        while (StringUtils.isNotBlank(line = br.readLine())) {
            String[] words = line.split("\t");
            pdMap.put(words[0], words[1]);
        }

        IOUtils.closeStream(br);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        String pName = pdMap.get(words[1]);
        keyOut.set(words[0] + "\t" + pName + "\t" + words[2]);
        context.write(keyOut, NullWritable.get());
    }
}
