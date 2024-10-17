package com.example.demo.bigdata.tutorial.mapred.task5;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-16 19:59
 * @description
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ComparableFlowBean implements WritableComparable<ComparableFlowBean> {

    private long upFlow;

    private long downFlow;

    private long sumFlow;

    @Override
    public int compareTo(ComparableFlowBean o) {
        return o.getSumFlow() == sumFlow ? Long.compare(upFlow, o.getUpFlow()) : Long.compare(o.getSumFlow(), sumFlow);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    @Override
    public String toString() {
        return this.upFlow + "\t" + this.downFlow + "\t" + this.sumFlow;
    }
}
