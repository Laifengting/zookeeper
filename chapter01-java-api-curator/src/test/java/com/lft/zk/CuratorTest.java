package com.lft.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class CuratorTest {
    
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
     * 创建节点：持久 临时 顺序 数据
     * 1. 基本创建
     * 2. 创建节点，带有数据
     * 3. 设置节点的类型
     * 4. 创建多级节点  /user/add
     */
    @Test
    public void testCreate1() throws Exception {
        // 1. 基本创建  如果创建节点，没有指定数据，则默认将当前客户端的 IP 作为数据存储
        String forPath = client.create().forPath("/app1");
        System.out.println(forPath);
    }
    
    /**
     * 创建节点：持久 临时 顺序 数据
     * 1. 基本创建
     * 2. 创建节点，带有数据
     * 3. 设置节点的类型
     * 4. 创建多级节点  /user/add
     */
    @Test
    public void testCreate2() throws Exception {
        // 2. 创建节点，带有数据
        String str = "Hello World";
        byte[] data = str.getBytes(StandardCharsets.UTF_8);
        String forPath = client.create().forPath("/app2", data);
        System.out.println(forPath);
    }
    
    /**
     * 创建节点：持久 临时 顺序 数据
     * 1. 基本创建
     * 2. 创建节点，带有数据
     * 3. 设置节点的类型
     * 4. 创建多级节点  /user/add
     */
    @Test
    public void testCreate3() throws Exception {
        // 3. 设置节点的类型
        // 默认类型：持久化的类型
        String forPath = client.create().withMode(CreateMode.EPHEMERAL).forPath("/app3");
        System.out.println(forPath);
    }
    
    /**
     * 创建节点：持久 临时 顺序 数据
     * 1. 基本创建
     * 2. 创建节点，带有数据
     * 3. 设置节点的类型
     * 4. 创建多级节点  /user/add
     */
    @Test
    public void testCreate4() throws Exception {
        // 4. 创建多级节点  /user/add
        // creatingParentsIfNeeded() 如果父节点不存在则创建。
        String forPath = client.create().creatingParentsIfNeeded().forPath("/app4/p1");
        System.out.println(forPath);
    }
    
    /**
     * 查询节点
     * 1. 查询数据：get
     * 2. 查询子节点：ls
     * 3. 查询节点状态信息：ls -s
     */
    @Test
    public void testSelect1() throws Exception {
        // 1. 查询数据：get
        byte[] bytes = client.getData().forPath("/app4/p1");
        System.out.println(new String(bytes));
    }
    
    /**
     * 查询节点
     * 1. 查询数据：get
     * 2. 查询子节点：ls
     * 3. 查询节点状态信息：ls -s
     */
    @Test
    public void testSelect2() throws Exception {
        // 2. 查询子节点：ls
        List<String> list = client.getChildren().forPath("/");
        System.out.println(list);
    }
    
    /**
     * 查询节点
     * 1. 查询数据：get
     * 2. 查询子节点：ls
     * 3. 查询节点状态信息：ls -s
     */
    @Test
    public void testSelect3() throws Exception {
        // 3. 查询节点状态信息：ls -s
        Stat stat = new Stat();
        byte[] bytes = client.getData().storingStatIn(stat).forPath("/app4");
        System.out.println(new String(bytes));
        System.out.println(stat);
    }
    
    /**
     * 修改数据
     * 1. 修改节点数据
     * 2. 根据版本号修改
     */
    @Test
    public void testUpdate1() throws Exception {
        byte[] bytes = client.getData().forPath("/app1");
        System.out.println(new String(bytes));
        
        System.out.println("======================");
        
        client.setData().forPath("/app1", "Hello World".getBytes(StandardCharsets.UTF_8));
        
        bytes = client.getData().forPath("/app1");
        System.out.println(new String(bytes));
    }
    
    /**
     * 修改数据
     * 1. 修改节点数据
     * 2. 根据版本号修改
     * *    版本号是通过查询获取的。目的是为了让其他客户端或线程不干扰。
     */
    @Test
    public void testUpdate2() throws Exception {
        // 获取原始数据，并保存版本号
        Stat stat = new Stat();
        System.out.println("旧版本号：" + stat.getVersion());
        byte[] bytes = client.getData().storingStatIn(stat).forPath("/app1");
        System.out.println("修改前数据：" + new String(bytes));
        
        System.out.println("======================");
        
        // 根据版本号修改数据
        client.setData().withVersion(stat.getVersion()).forPath("/app1", "Hello Tom".getBytes(StandardCharsets.UTF_8));
        
        bytes = client.getData().storingStatIn(stat).forPath("/app1");
        System.out.println("新版本号：" + stat.getVersion());
        System.out.println("修改后数据：" + new String(bytes));
    }
    
    /**
     * 删除节点：delete deleteAll
     * 1. 删除单个节点  client.delete().forPath("/app1");
     * 2. 删除带有子节点的节点  client.delete().deletingChildrenIfNeeded().forPath("/app1");
     * 3. 必须成功的删除：为了防止网络抖动，本质就是重试   client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/app1");
     * 4. 回调  client.delete().guaranteed().deletingChildrenIfNeeded().inBackground(backgroundCallback).forPath("/app1");
     */
    @Test
    public void testDelete1() throws Exception {
        // 1. 删除单个节点
        client.delete().forPath("/app1");
    }
    
    /**
     * 删除节点：delete deleteAll
     * 1. 删除单个节点  client.delete().forPath("/app1");
     * 2. 删除带有子节点的节点  client.delete().deletingChildrenIfNeeded().forPath("/app1");
     * 3. 必须成功的删除：为了防止网络抖动，本质就是重试   client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/app1");
     * 4. 回调  client.delete().guaranteed().deletingChildrenIfNeeded().inBackground(backgroundCallback).forPath("/app1");
     */
    @Test
    public void testDelete2() throws Exception {
        // 2. 删除带有子节点的节点
        client.delete().deletingChildrenIfNeeded().forPath("/app4");
    }
    
    /**
     * 删除节点：delete deleteAll
     * 1. 删除单个节点  client.delete().forPath("/app1");
     * 2. 删除带有子节点的节点  client.delete().deletingChildrenIfNeeded().forPath("/app1");
     * 3. 必须成功的删除：为了防止网络抖动，本质就是重试   client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/app1");
     * 4. 回调  client.delete().guaranteed().deletingChildrenIfNeeded().inBackground(backgroundCallback).forPath("/app1");
     */
    @Test
    public void testDelete3() throws Exception {
        // 3. 必须成功的删除
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/app2");
    }
    
    /**
     * 删除节点：delete deleteAll
     * 1. 删除单个节点  client.delete().forPath("/app1");
     * 2. 删除带有子节点的节点  client.delete().deletingChildrenIfNeeded().forPath("/app1");
     * 3. 必须成功的删除：为了防止网络抖动，本质就是重试   client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/app1");
     * 4. 回调  client.delete().guaranteed().deletingChildrenIfNeeded().inBackground(backgroundCallback).forPath("/app1");
     */
    @Test
    public void testDelete4() throws Exception {
        // 4. 回调
        BackgroundCallback backgroundCallback = new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("执行了 backgroundCallback 对象的 processResult 方法");
                System.out.println("事件对象是：" + event);
            }
        };
        client.delete().guaranteed().deletingChildrenIfNeeded().inBackground(backgroundCallback).forPath("/app1");
    }
    
}
