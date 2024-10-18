package com.example.demo.bigdata.tutorial.jdk.rpc.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/18 16:53
 * @Version: 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RpcEntity {
    private static final long serialVersionUID = -123L;

    private String id;
    private String name;
}
