package com.example.demo.bigdata.tutorial.flink.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/23 9:23
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Double vc;
    private long ts;

    @Override
    public String toString() {
        return "WaterSensor{id=" + id + ", vc=" + vc + ", ts=" + new Timestamp(ts) + "}";
    }
}
