package com.example.demo.bigdata.tutorial.rpc.task1;

/**
 * @Description: hadoop rpc
 * @Author: Chenyang on 2024/10/18 17:39
 * @Version: 1.0
 */
public interface RpcProtocol {

    long versionID = 123L;

    /**
     * 新建目录
     */
    void mkdir(String path);
}
