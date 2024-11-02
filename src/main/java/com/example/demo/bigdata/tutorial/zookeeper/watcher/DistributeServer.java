package com.example.demo.bigdata.tutorial.zookeeper.watcher;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/02 21:14
 * @Version: 1.0
 */
public class DistributeServer {

    private static final String ZK_ADDR = "hadoop202:2181,hadoop203:2181,hadoop204:2181";
    private static final int SESSION_TIMEOUT = 2000;
    private ZooKeeper zkServer;

    public static void main(String[] args) throws Exception {
        DistributeServer server = new DistributeServer();

        // 获取zk连接
        server.connect();
        // 注册服务器到zk集群
        server.register(args[0]);
        // 启动业务逻辑(睡眠)
        server.business();
    }

    public void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    public void register(String hostname) throws KeeperException, InterruptedException {
        zkServer.create("/servers/" + hostname, hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public void connect() throws IOException {
        zkServer = new ZooKeeper(ZK_ADDR, SESSION_TIMEOUT, event -> {
        });
    }
}
