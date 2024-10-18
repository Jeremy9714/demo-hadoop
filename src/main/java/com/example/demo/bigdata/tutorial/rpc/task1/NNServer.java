package com.example.demo.bigdata.tutorial.rpc.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/18 17:40
 * @Version: 1.0
 */
public class NNServer implements RpcProtocol {

    public static void main(String[] args) throws IOException {
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(23456)
                .setProtocol(RpcProtocol.class)
                .setInstance(new NNServer())
                .build();

        System.out.println("=====服务器启动=====");
        server.start();
    }

    @Override
    public void mkdir(String path) {
        System.out.println("=====创建路径:" + path + "=====");
    }
}
