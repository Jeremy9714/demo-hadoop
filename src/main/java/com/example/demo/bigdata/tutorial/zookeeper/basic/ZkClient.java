package com.example.demo.bigdata.tutorial.zookeeper.basic;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @Description: zk cli
 * @Author: Chenyang on 2024/11/02 17:06
 * @Version: 1.0
 */
public class ZkClient {

    private static final String ZK_ADDR = "hadoop202:2181,hadoop203:2181,hadoop204:2181";
    private static final int SESSION_TIMEOUT = 2000;
    private ZooKeeper zkClient;

    @Before
    public void process() throws IOException {
        zkClient = new ZooKeeper(ZK_ADDR, SESSION_TIMEOUT, new MyWatcher());
    }

    @Test
    public void create() throws KeeperException, InterruptedException {
        String zNode = zkClient.create("/mytest", "workplace".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("路径为: " + zNode);
    }

    @Test
    public void getData() throws KeeperException, InterruptedException {
        zkClient.getData("/mytest", true, null);
        zkClient.setData("/mytest", "newWorkplace".getBytes(), -1);
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        System.out.println("=====子目录=====");
        for (String child : children) {
            System.out.println(child);
        }
    }

    @Test
    public void exists() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/mytest", false);
        System.out.println(exists == null ? "not exists" : "exists");
    }

    public static class MyWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            System.out.println("事件触发: " + event.getPath() + ", 事件类型: " + event.getType());
        }
    }
}
