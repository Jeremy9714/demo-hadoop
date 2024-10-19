package com.example.demo.bigdata.tutorial.hadoop.mapred.task8;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Chenyang
 * @create 2024-10-17 10:57
 * @description
 */
@Data
@NoArgsConstructor
public class TableBean implements Writable {

    private String id;
    private String pId;
    private String pName;
    private int amount;
    private String flag;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pId);
        out.writeUTF(pName);
        out.writeInt(amount);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pId = in.readUTF();
        this.pName = in.readUTF();
        this.amount = in.readInt();
        this.flag = in.readUTF();
    }

    @Override
    public String toString() {
        return id + "\t" + pName + "\t" + amount;
    }
}
