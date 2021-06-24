package com.lft.zk.service;

import com.lft.zk.util.ZkClientUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;

/**
 * 服务端 主要提供了 客户端需要的一个时间查询服务
 * 服务端 向 zookeeper 中建立临时节点
 */
public class Service {
    
    /**
     * 创建 zk 客户端连接
     */
    private CuratorFramework connectToZk() throws Exception {
        // 创建一个 zk 客户端
        CuratorFramework client = ZkClientUtils.getClient();
        // 开启连接
        client.start();
        
        // 判断临时节点父目录是否存在
        Stat stat = client.checkExists().forPath("/servers");
        
        // 如果不存在先创建
        if (stat == null) {
            // 创建服务端的临时节点父目录
            client.create().withMode(CreateMode.PERSISTENT).forPath("/servers");
        }
        
        return client;
    }
    
    /**
     * 在 zk 中保存服务器相关信息
     */
    private void saveServerInfoToZk(String ip, String port) throws Exception {
        CuratorFramework client = connectToZk();
        String path = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                            .forPath("/servers/server", (ip + ":" + port).getBytes(StandardCharsets.UTF_8));
        System.out.println(path);
    }
    
    public static void main(String[] args) throws Exception {
        Service service = new Service();
        service.saveServerInfoToZk(args[0], args[1]);
        new TimeService(Integer.parseInt(args[1])).start();
    }
}
