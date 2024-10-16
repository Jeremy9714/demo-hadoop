package com.example.demo.bigdata.tutorial.mapred.task4;

import com.example.demo.bigdata.tutorial.mapred.task2.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Chenyang
 * @create 2024-10-16 18:29
 * @description
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phoneNum = text.toString();
        String prefix = phoneNum.substring(0, 3);

        int partition;
        if ("136".equals(prefix)) {
            partition = 0;
        } else if ("137".equals(prefix)) {
            partition = 1;
        } else if ("138".equals(prefix)) {
            partition = 2;
        } else if ("139".equals(prefix)) {
            partition = 3;
        } else {
            partition = 4;
        }
        return partition;
    }
}
