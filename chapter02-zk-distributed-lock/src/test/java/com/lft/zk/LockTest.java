package com.lft.zk;

import com.lft.zk.entity.Ticket;

public class LockTest {
    
    public static void main(String[] args) {
        Ticket ticket = new Ticket();
        
        Thread thread1 = new Thread(ticket, "携程");
        Thread thread2 = new Thread(ticket, "飞猪");
        
        thread1.start();
        thread2.start();
    }
}
