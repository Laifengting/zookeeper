package com.lft.zk.lock;

import com.lft.zk.util.ZkClientUtils;
import org.apache.curator.framework.api.ExistsBuilder;
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

/**
 * 抢锁
 * *    在 zk 中创建临时顺序节点，并获取到序号
 * *    判断自己创建节点序号是否是当前节点最小序号。
 * *        如果是则是获取到锁。执行相关操作，最后要释放锁。
 * *        如果不是，当前线程需要等待，等待你前一个序号的节点被删除。然后再次判断自己是否是最小节点。
 * 释放锁
 */
public class ZkLock {
    private String prefix = "/locks/";
    
    private ThreadLocal<Map<String, String>> threadLocal = new ThreadLocal<>();
    
    // private String esPath = null;
    // private String preNodeTotalPath = null;
    
    private CountDownLatch countDownLatch = null;
    
    public ZkLock() {
        // 先创建父节点 /locks 是永久的。
        try {
            ExistsBuilder existsBuilder = ZkClientUtils.getClient().checkExists();
            Stat stat = existsBuilder.forPath(prefix.substring(0, prefix.length() - 1));
            if (stat == null) {
                ZkClientUtils.getClient().create().forPath(prefix.substring(0, prefix.length() - 1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // 把抢锁过程分为两部分，一部分是创建节点 ，比较序号，另一部分是等待锁
    
    /**
     * 完整获取锁方法
     */
    public void lock() throws Exception {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 首先调用 tryLock()
        if (tryLock()) {
            // 说明获取到锁
            System.out.println(Thread.currentThread().getName() + " 线程，获取到锁");
        } else {
            // 没有获取到锁
            System.out.println(Thread.currentThread().getName() + " 线程，获取锁失败");
            // 等待锁
            waitLock();
            lock();
        }
        
    }
    
    /**
     * 尝试获取锁
     */
    public boolean tryLock() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Map<String, String> map = threadLocal.get();
        if (map == null) {
            map = new HashMap<>(10);
            threadLocal.set(map);
        }
        String esPath = map.get("esPath");
        
        try {
            if (esPath == null || "".equals(esPath)) {
                // 创建当前线程的临时顺序节点 /locks/ 没有指定名称就直接是序号
                esPath = ZkClientUtils.getClient().create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(prefix);
                System.out.println(Thread.currentThread().getName() + " 线程，创建临时顺序节点：" + esPath);
                map.put("esPath", esPath);
            }
            
            // 获取 locks 节点下的所有子节点。
            List<String> childNodes = ZkClientUtils.getClient().getChildren().forPath(prefix.substring(0, prefix.length() - 1));
            
            // 对节点信息进行排序（默认升序）
            Collections.sort(childNodes);
            System.out.println(Thread.currentThread().getName() + " 线程，排序后的所有子节点：" + childNodes);
            
            // 获取最小的节点
            String minNode = childNodes.get(0);
            System.out.println(Thread.currentThread().getName() + " 线程，获取到的最小序号节点：" + prefix + minNode);
            // 判断自己创建的节点是不是最小序号一致
            if (esPath.equals(prefix + minNode)) {
                // 当前线程创建的就是序号最小节点
                return true;
            } else {
                // 说明最小节点不是自己创建的，监控自己序号前一个的节点。
                String esPathSubString = esPath.substring(prefix.length());
                int index = Collections.binarySearch(childNodes, esPathSubString);
                // 前一个节点（不包含父节点）
                String preChildPath = childNodes.get(index - 1);
                // 前一个节点的完整路径
                String preNodeTotalPath = prefix + preChildPath;
                map.put("preNodeTotalPath", preNodeTotalPath);
                System.out.println(Thread.currentThread().getName() + " 线程，前一个节点的完整路径：" + preNodeTotalPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    
    /**
     * 等待之前节点释放锁，如何判断锁被释放，需要唤醒线程继续尝试 tryLock
     */
    public void waitLock() throws Exception {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Map<String, String> map = threadLocal.get();
        String preNodeTotalPath = map.get("preNodeTotalPath");
        // 监控前一个节点
        CuratorCache curatorCache = CuratorCache.build(ZkClientUtils.getClient(), preNodeTotalPath);
        // 添加监听器
        // CuratorCacheListener.builder().forNodeCache();
        // CuratorCacheListener.builder().forPathChildrenCache();
        // CuratorCacheListener.builder().forTreeCache();
        CuratorCacheListener listener = new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                // 当事件类型是 节点删除时
                // if (type == Type.NODE_DELETED) {
                // 提醒当前线程再次去获取锁
                System.out.println(Thread.currentThread().getName() + " 线程：前一个节点被删除了");
                countDownLatch.countDown(); // 每执行一次减 1
                // }
            }
        };
        curatorCache.listenable().addListener(listener);
        
        // 在监听的通知没来之前，该线程应该是等待状态，先判断一次上一个节点是否还存在。
        ExistsBuilder existsBuilder = ZkClientUtils.getClient().checkExists();
        Stat stat = existsBuilder.forPath(preNodeTotalPath);
        if (stat != null) {
            // 上一个节点存在
            // 开始等待，CountDownLatch 线程同步计数器，初始值设置为1
            countDownLatch = new CountDownLatch(1);
            countDownLatch.await(); // 阻塞直到值变为 0
            // // 再次获取锁
            // lock();
        } else {
            // 去获取锁
            // // 再次获取锁
            // lock();
        }
        // 解除监听器
        curatorCache.listenable().removeListener(listener);
        
    }
    
    /**
     * 释放锁
     */
    public void unLock() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Map<String, String> map = threadLocal.get();
        String esPath = map.get("esPath");
        System.out.println(Thread.currentThread().getName() + " 线程，释放当前的节点：" + esPath);
        try {
            // map.remove("esPath");
            // map.remove("preNodeTotalPath");
            // 删除自己的当前节点
            ZkClientUtils.getClient().delete().guaranteed().deletingChildrenIfNeeded().forPath(esPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
