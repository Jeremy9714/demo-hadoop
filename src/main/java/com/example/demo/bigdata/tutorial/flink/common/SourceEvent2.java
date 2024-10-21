package com.example.demo.bigdata.tutorial.flink.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/21 11:48
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SourceEvent2 implements Serializable {

    public static final long serialVersionUID = -3453L;

    private String name;
    private String url;
    private long timestamp;

    @Override
    public String toString() {
        return "SourceEvent[name=" + name + ", url=" + url + ", timestamp=" + new Timestamp(timestamp) + "]";
    }

}
