package com.example.demo.bigdata.tutorial.flink.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/21 17:10
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlinkEvent {

    private String id;
    private String name;
    private Integer age;

}
