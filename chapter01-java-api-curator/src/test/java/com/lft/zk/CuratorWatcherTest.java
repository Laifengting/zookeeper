package com.lft.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CuratorWatcherTest {
    
    CuratorFramework client;
    
    @Before
    public void testConnect() {
        
        // * @param connectString       连接服务地址和端口号和集合 "127.0.0.1:2181,127.0.0.1:2181,127.0.0.1:2181"
        // * @param sessionTimeoutMs    会话超时单位 ms
        // * @param connectionTimeoutMs 连接超时单位 ms
        // * @param retryPolicy         重试策略
        // 创建客户端的第一种方式
        // CuratorFramework client = CuratorFrameworkFactory
        //         .newClient(
        //                 "127.0.0.1:2181",
        //                 60 * 1000,
        //                 15 * 1000,
        //                 new ExponentialBackoffRetry(3000, 10));
        
        // 创建客户端的第二种方式
        client = CuratorFrameworkFactory
                .builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .namespace("com.lft.zk")
                .retryPolicy(new ExponentialBackoffRetry(3000, 10)).build();
        
        // 开启连接
        client.start();
    }
    
    @After
    public void closeClient() {
        if (client != null) {
            client.close();
        }
    }
    
    /**
     * NodeCache：给指定一个节点注册监听器
     * 监听 当前节点的变化
     */
    @Test
    public void testNodeCache() throws Exception {
        // 1. 创建 NodeCache 对象
        NodeCache nodeCache = new NodeCache(client, "/app1", false);
        
        // 2. 注册监听
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("节点变化了~");
                
                // 获取修改节点后的数据
                byte[] data = nodeCache.getCurrentData().getData();
                System.out.println("当前的数据是：" + new String(data));
                System.out.println("当前的节点路径是：" + nodeCache.getCurrentData().getPath());
                System.out.println("当前的数据长度是：" + nodeCache.getCurrentData().getStat().getDataLength());
            }
        });
        
        // 3. 开启监听，如果设置为 true，则开启监听时，加载缓存数据。
        nodeCache.start(true);
        
        while (true) {
        }
    }
    
    /**
     * PathChildrenCache：监听某个节点的所有子节点们
     * 监听 当前节点所有子节点的变化
     */
    @Test
    public void testPathChildrenCache() throws Exception {
        // 1. 创建 PathChildrenCache 对象
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/app1", true);
        
        // 2. 注册监听
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                System.out.println("节点变化了~");
                System.out.println("事件是：" + event);
                
                // 监听子节点的数据变化，并且拿到更新后的数据
                PathChildrenCacheEvent.Type type = event.getType();
                
                if (type.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                    System.out.println("执行了子节点的更新操作了~~~");
                    System.out.println("数据是：" + new String(event.getData().getData()));
                    System.out.println("数据节点是：" + event.getData().getPath());
                }
            }
        });
        
        // 3. 开启监听，如果设置为 true，则开启监听时，加载缓存数据。
        pathChildrenCache.start();
        
        while (true) {
        }
    }
    
    /**
     * TreeCache：监听某个节点和所有子节点们
     * 监听 当前节点和所有子节点的变化
     */
    @Test
    public void testTreeCache() throws Exception {
        // 1. 创建 TreeCache 对象
        TreeCache treeCache = new TreeCache(client, "/app1");
        
        // 2. 注册监听
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                System.out.println("节点变化了~~");
                System.out.println("事件是：" + event);
                TreeCacheEvent.Type type = event.getType();
                if (type.equals(TreeCacheEvent.Type.NODE_UPDATED)) {
                    System.out.println("执行了子节点的更新操作了~~~");
                    System.out.println("数据是：" + new String(event.getData().getData()));
                    System.out.println("数据节点是：" + event.getData().getPath());
                }
            }
        });
        
        // 3. 开启监听，如果设置为 true，则开启监听时，加载缓存数据。
        treeCache.start();
        
        while (true) {
        }
    }
    
    /**
     * CuratorCache：给指定一个节点注册监听器
     */
    @Test
    public void testCuratorCache1() throws Exception {
        // 1. 创建 NodeCache 对象
        CuratorCache curatorCache = CuratorCache.build(client, "/app1", CuratorCache.Options.DO_NOT_CLEAR_ON_CLOSE);
        
        // 2. 注册监听
        curatorCache.listenable().addListener(new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                // System.out.println("节点变化了~");
                // System.out.println("事件类型：" + type);
                // System.out.println("旧数据为：" + new String(oldData.getData()));
                // System.out.println("新数据为：" + new String(data.getData()));
                System.out.println("======================= 监视到服务器数据变化 =======================");
                // 如果旧数据不为空
                if (oldData != null) {
                    // 转化为字符串
                    String oldDataString = new String(oldData.getData());
                    System.out.println("旧数据为：" + oldDataString);
                    // 从信息表中找到旧数据的索引
                    // int i = infoList.indexOf(oldDataString);
                    // 如果新数据为空
                    if (data == null) {
                        // infoList.remove(i);
                    } else {
                        String newDataString = new String(data.getData());
                        System.out.println("新数据为：" + newDataString);
                        // infoList.set(i, new String(data.getData()));
                    }
                } else {
                    if (data != null) {
                        // infoList.add(new String(data.getData()));
                        String newDataString = new String(data.getData());
                        System.out.println(newDataString);
                    }
                }
            }
        });
        
        // 3. 开启监听
        curatorCache.start();
        
        while (true) {
        }
    }
    
}
