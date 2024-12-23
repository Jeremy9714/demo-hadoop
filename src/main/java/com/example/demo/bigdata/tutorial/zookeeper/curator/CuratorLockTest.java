package com.example.demo.bigdata.tutorial.zookeeper.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/02 23:24
 * @Version: 1.0
 */
public class CuratorLockTest {
    public static void main(String[] args) {

        // 创建分布式锁1
        InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(), "/locks");

        // 创建分布式锁2
        InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(), "/locks");

        new Thread(() -> {
            try {
                lock1.acquire();
                System.out.println("线程1 获取到锁！");

                lock1.acquire();
                System.out.println("线程1 再次获取到锁！");

                Thread.sleep(5 * 1000);

                lock1.release();
                System.out.println("线程1 释放锁！");

                lock1.release();
                System.out.println("线程1 再次释放锁！");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                lock2.acquire();
                System.out.println("线程2 获取到锁！");

                lock2.acquire();
                System.out.println("线程2 再次获取到锁！");

                Thread.sleep(5 * 1000);

                lock2.release();
                System.out.println("线程2 释放锁！");

                lock2.release();
                System.out.println("线程2 再次释放锁！");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static CuratorFramework getCuratorFramework() {
        ExponentialBackoffRetry policy = new ExponentialBackoffRetry(3000, 3);

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("hadoop202:2181,hadoop203:2181,hadoop204:2181")
                .connectionTimeoutMs(2000)
                .sessionTimeoutMs(2000)
                .retryPolicy(policy)
                .build();

        client.start();
        System.out.println("zookeeper启动成功");

        return client;
    }
}
