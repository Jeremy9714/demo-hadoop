package com.example.demo.bigdata.tutorial.jdk.rpc.service;

import com.example.demo.bigdata.tutorial.jdk.rpc.entity.RpcEntity;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/18 16:56
 * @Version: 1.0
 */
public interface RpcEntityService {

    RpcEntity getEntityById(String id);
}
