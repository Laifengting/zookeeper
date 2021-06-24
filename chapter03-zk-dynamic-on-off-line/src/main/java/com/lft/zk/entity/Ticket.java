package com.lft.zk.entity;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

/**
 * Class Name:      Ticket
 * Package Name:    com.lft.zk.entity
 * <p>
 * Function: 		A {@code Ticket} object With Some FUNCTION.
 * Date:            2021-06-23 11:47
 * <p>
 * @author Laifengting / E-mail:laifengting@foxmail.com
 * @version 1.0.0
 * @since JDK 8
 */
public class Ticket implements Runnable {
    
    /**
     * 数据库的票数
     */
    private Integer tickets = 10;
    
    /**
     * 分布式锁
     */
    private InterProcessMutex lock;
    
    public Ticket() {
        // 创建客户端的第二种方式
        CuratorFramework client = CuratorFrameworkFactory
                .builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(new ExponentialBackoffRetry(3000, 10))
                .build();
        
        // 开启连接
        client.start();
        System.out.println(client);
        lock = new InterProcessMutex(client, "/locks");
        
    }
    
    @Override
    public void run() {
        while (true) {
            try {
                // 获取锁
                lock.acquire(2, TimeUnit.SECONDS);
                if (tickets > 0) {
                    System.out.println(Thread.currentThread().getName() + " : " + tickets);
                    Thread.sleep(1000);
                    tickets--;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // 释放锁
                try {
                    lock.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
