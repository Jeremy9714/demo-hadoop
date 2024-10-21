package com.example.demo.bigdata.tutorial.flink.chapter2_streamapi.task1;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/20 21:25
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CustomFlinkEvent implements Serializable {
    public static final long serialVersionUID = -1L;

    private int id;
    private String name;
    private long timestamp;
}
