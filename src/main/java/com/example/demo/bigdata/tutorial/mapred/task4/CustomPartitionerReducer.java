package com.example.demo.bigdata.tutorial.mapred.task4;

import com.example.demo.bigdata.tutorial.mapred.task2.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 18:21
 * @description
 */
public class CustomPartitionerReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean valueOut = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long totalUpFlow = 0L;
        long totalDownFlow = 0L;

        for (FlowBean value : values) {
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
        }

        valueOut.setUpFlow(totalUpFlow);
        valueOut.setDownFlow(totalDownFlow);
        valueOut.setSumFlow();

        context.write(key, valueOut);
    }
}
