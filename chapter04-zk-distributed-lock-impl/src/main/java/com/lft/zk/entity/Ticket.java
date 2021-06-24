package com.lft.zk.entity;

import com.lft.zk.lock.MyLock;

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
    private MyLock lock = new MyLock();
    
    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                // 获取锁
                lock.lock();
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
                    lock.unlock();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
