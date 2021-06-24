package com.lft.zk.lock;

import com.lft.zk.util.ZkClientUtils;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class MyLock {
    private final String prefix = "/locks/";
    
    private final ThreadLocal<Map<String, Object>> threadLocal = new ThreadLocal<>();
    
    CountDownLatch countDownLatch = null;
    
    public MyLock() {
        try {
            Stat stat = ZkClientUtils.getClient().checkExists().forPath(prefix.substring(0, prefix.length() - 1));
            if (stat == null) {
                ZkClientUtils.getClient().create().forPath(prefix.substring(0, prefix.length() - 1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void lock() {
        // 尝试加锁
        if (trylock()) {
            System.out.println(Thread.currentThread().getName() + " 线程，获取到锁");
        } else {
            System.out.println(Thread.currentThread().getName() + " 线程，获取锁失败");
            // 等待获取锁
            waitLock();
            // 再进行加锁
            lock();
        }
    }
    
    private boolean trylock() {
        Map<String, Object> map = threadLocal.get();
        if (map == null) {
            map = new HashMap<>(10);
            threadLocal.set(map);
        }
        
        try {
            String esNodePath = (String) map.get("esNodePath");
            if (esNodePath == null || "".equals(esNodePath)) {
                // 创建临时有序节点
                esNodePath = ZkClientUtils.getClient().create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(prefix);
                map.put("esNodePath", esNodePath);
                System.out.println(Thread.currentThread().getName() + " 线程，创建临时有序节点：" + esNodePath);
            }
            
            // 获取 /locks 下所有子节点
            List<String> childNodePathList = ZkClientUtils.getClient().getChildren().forPath(prefix.substring(0, prefix.length() - 1));
            // 给集合排序
            Collections.sort(childNodePathList);
            System.out.println(Thread.currentThread().getName() + " 线程，排序后的所有子节点路径：" + childNodePathList);
            // 获取最小的节点
            String minNodePath = childNodePathList.get(0);
            System.out.println(Thread.currentThread().getName() + " 线程，最小子节点路径：" + minNodePath);
            // 比较当前节点是否就是最小的节点
            if (esNodePath.equals(prefix + minNodePath)) {
                // 如果是表示当前线程获取到锁。
                return true;
            } else {
                // 如果不是表示当前线程没有获取到。
                // 找到当前节点的前一个节点
                int index = childNodePathList.indexOf(esNodePath.substring(prefix.length()));
                String preNodePath = childNodePathList.get(index - 1);
                System.out.println(Thread.currentThread().getName() + " 线程，当前节点前一节点的路径：" + preNodePath);
                map.put("preNodePath", preNodePath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    
    private void waitLock() {
        Map<String, Object> map = threadLocal.get();
        String preNodePath = (String) map.get("preNodePath");
        CuratorCache curatorCache = CuratorCache.build(ZkClientUtils.getClient(), prefix + preNodePath);
        CuratorCacheListener listener = new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                if (type == Type.NODE_DELETED) {
                    countDownLatch.countDown();
                    // map.remove("countDownLatch");
                }
            }
        };
        curatorCache.listenable().addListener(listener);
        try {
            countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        curatorCache.listenable().removeListener(listener);
    }
    
    public void unlock() {
        Map<String, Object> map = threadLocal.get();
        String esNodePath = (String) map.get("esNodePath");
        // 释放锁
        try {
            ZkClientUtils.getClient().delete().forPath(esNodePath);
            map.clear();
            System.out.println(Thread.currentThread().getName() + " 线程，解锁删除掉当前节点：" + esNodePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
