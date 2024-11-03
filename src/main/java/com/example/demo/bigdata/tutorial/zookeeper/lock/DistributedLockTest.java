package com.example.demo.bigdata.tutorial.zookeeper.lock;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/02 23:10
 * @Version: 1.0
 */
public class DistributedLockTest {
    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        DistributedLock lock1 = new DistributedLock();
        DistributedLock lock2 = new DistributedLock();
        new Thread(() -> {
            try {
                lock1.zkLock();
                System.out.println("线程1启动, 获取到锁！");
                Thread.sleep(5 * 1000);
                lock1.zkUnlock();
                System.out.println("线程1结束, 释放锁！");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                lock2.zkLock();
                System.out.println("线程2启动, 获取到锁！");
                Thread.sleep(5 * 1000);
                lock2.zkUnlock();
                System.out.println("线程2结束, 释放锁！");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }
}
