package com.example.demo.bigdata.tutorial.flink.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/21 10:17
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SourceEvent1 implements Serializable {
    public static final long serialVersionUID = -346346L;

    private Integer id;
    private String name;
    private long timestamp;
    
    @Override
    public String toString(){
        return id + ", " + name + ", " + new Timestamp(timestamp);
    }
}
