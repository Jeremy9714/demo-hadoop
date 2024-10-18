package com.example.demo.bigdata.tutorial.jdk.rpc.service.impl;

import com.example.demo.bigdata.common.util.CTools;
import com.example.demo.bigdata.tutorial.jdk.rpc.entity.RpcEntity;
import com.example.demo.bigdata.tutorial.jdk.rpc.service.RpcEntityService;

/**
 * @Description: rpc服务实现
 * @Author: Chenyang on 2024/10/18 16:57
 * @Version: 1.0
 */
public class RpcEntityServiceImpl implements RpcEntityService {
    @Override
    public RpcEntity getEntityById(String id) {
        return new RpcEntity(CTools.getUUID(), "Jeremy");
    }
}
