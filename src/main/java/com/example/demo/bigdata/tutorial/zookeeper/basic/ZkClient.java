package com.example.demo.bigdata.tutorial.zookeeper.basic;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;

/**
 * @Description: zk cli
 * @Author: Chenyang on 2024/11/02 17:06
 * @Version: 1.0
 */
public class ZkClient {

    private String connectString = "hadoop202:2181,hadoop203:2181,hadoop204:2181";
    private int sessionTimeout = 2000;

    @Test
    public void process() throws IOException {
        ZooKeeper zooKeeper = new ZooKeeper(connectString, sessionTimeout, event -> {

        });
    }
}
