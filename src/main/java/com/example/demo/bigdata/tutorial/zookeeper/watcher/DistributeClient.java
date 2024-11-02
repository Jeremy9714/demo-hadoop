package com.example.demo.bigdata.tutorial.zookeeper.watcher;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/02 21:30
 * @Version: 1.0
 */
public class DistributeClient {

    private static final String ZK_ADDR = "hadoop202:2181,hadoop203:2181,hadoop204:2181";
    private static final int SESSION_TIMEOUT = 2000;
    private ZooKeeper zkClient;

    public static void main(String[] args) throws Exception {

        DistributeClient client = new DistributeClient();
        // 获取zk连接
        client.connect();

        // 监听路径下子节点的增加和删除
        client.getServerList();

        // 业务逻辑(睡眠)
        client.business();
    }

    public void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    public void getServerList() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/servers", true);
        List<String> list = new ArrayList<>();
        for (String child : children) {
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            list.add(new String(data == null ? new byte[]{} : data));
        }

        System.out.println("节点信息: " + list);
    }

    public void connect() throws IOException {
        zkClient = new ZooKeeper(ZK_ADDR, SESSION_TIMEOUT, event -> {
            System.out.println("====触发事件====");
            try {
                getServerList();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

}
