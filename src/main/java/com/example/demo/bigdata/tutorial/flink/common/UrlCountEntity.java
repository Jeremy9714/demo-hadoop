package com.example.demo.bigdata.tutorial.flink.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/22 21:39
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UrlCountEntity {

    private String url;
    private long count;
    private long windowStart;
    private long windowEnd;

    @Override
    public String toString() {
        return "UrlCountEntity{url=" + url + ", count=" + count + ", windowStart=" +
                new Timestamp(windowStart) + ", windowEnd=" + new Timestamp(windowEnd) + "}";
    }
}
