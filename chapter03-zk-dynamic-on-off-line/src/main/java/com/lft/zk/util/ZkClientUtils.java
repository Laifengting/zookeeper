package com.lft.zk.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZkClientUtils {
    private static CuratorFramework client;
    
    static {
        // 创建客户端的第二种方式
        client = CuratorFrameworkFactory
                .builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(new ExponentialBackoffRetry(3000, 10))
                .build();
    }
    
    public static CuratorFramework getClient() {
        return client;
    }
    
    public static boolean closeClient(CuratorFramework client) {
        if (client != null) {
            try {
                client.close();
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return false;
    }
    
}
