package com.example.demo.bigdata.tutorial.rpc.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/18 17:42
 * @Version: 1.0
 */
public class HDFSClient {
    public static void main(String[] args) throws Exception {
        // 获取客户端对象
        RpcProtocol client = RPC.getProxy(RpcProtocol.class, RpcProtocol.versionID,
                new InetSocketAddress("localhost", 23456), new Configuration());

        System.out.println("=====客户端开始工作=====");
        client.mkdir("/newpath");
    }
}
