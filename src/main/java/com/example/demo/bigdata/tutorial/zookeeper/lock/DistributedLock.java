package com.example.demo.bigdata.tutorial.zookeeper.lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import static org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/02 22:43
 * @Version: 1.0
 */
public class DistributedLock {

    private static final String ZK_ADDR = "hadoop202:2181,hadoop203:2181,hadoop204:2181";
    private static final int SESSION_TIMEOUT = 2000;
    private CountDownLatch connectLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);
    private ZooKeeper zkClient;
    private String currentNode;
    private String previousNode;

    public DistributedLock() throws IOException, KeeperException, InterruptedException {
        // 获取连接
        zkClient = new ZooKeeper(ZK_ADDR, SESSION_TIMEOUT, event -> {
            // 连接上zk, 释放锁
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectLatch.countDown();
            }
            // 前一个节点删除, 释放锁
            if (event.getType() == Watcher.Event.EventType.NodeDeleted && event.getPath().equals(previousNode)) {
                waitLatch.countDown();
            }
        });

        // 等待zk连接完毕
        connectLatch.await();

        // 判断根节点是否存在
        Stat stat = zkClient.exists("/locks", true);
        if (stat == null) {
            zkClient.create("/locks", "locks".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    // 加锁
    public void zkLock() {
        // 创建临时顺序节点
        try {
            currentNode = zkClient.create("/locks/seq-", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            // 判断当前节点位置
            List<String> locks = zkClient.getChildren("/locks", false);
            if (locks.size() == 1) {
                return;
            } else {
                Collections.sort(locks);

                // 获取节点名称
                String nodePath = currentNode.substring("/locks/".length());
                int index = locks.indexOf(nodePath);
                if (index == -1) {
                    System.out.println("数据异常");
                } else if (index == 0) {
                    return;
                } else {
                    previousNode = "/locks/" + locks.get(index - 1);
                    // 监听上一个节点
                    zkClient.getData(previousNode, true, null);
                    // 等待监听
                    waitLatch.await();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    // 解锁
    public void zkUnlock() {
        try {
            zkClient.delete(currentNode, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
