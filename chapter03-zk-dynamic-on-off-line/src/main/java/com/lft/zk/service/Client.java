package com.lft.zk.service;

import com.lft.zk.util.ZkClientUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 注册监听 zk 指定目录
 * 维护自己本地一个 servers 信息，收到通知要进行更新。
 * 发送时间查询请求并接受服务端返回的数据。
 */
public class Client {
    
    private List<String> infoList = new ArrayList<>();
    
    /**
     * 创建 zk 客户端连接
     */
    private void connectToZk() throws Exception {
        // 创建一个 zk 客户端
        CuratorFramework client = ZkClientUtils.getClient();
        // 开启连接
        client.start();
        
        // 第一次获取服务器信息
        List<String> list = client.getChildren().forPath("/servers");
        
        for (String s : list) {
            System.out.println(s);
            // 存储的 ip + port
            byte[] bytes = client.getData().forPath("/servers/" + s);
            infoList.add(new String(bytes));
        }
        
        // 对 servers 目录进行监听
        // 1. 创建 NodeCache 对象
        CuratorCache curatorCache = CuratorCache.build(client, "/servers", CuratorCache.Options.DO_NOT_CLEAR_ON_CLOSE);
        
        // 2. 注册监听
        curatorCache.listenable().addListener(new CuratorCacheListener() {
            @Override
            public void event(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
                System.out.println("======================= 监视到服务器数据变化 =======================");
                // 如果旧数据不为空
                if (oldData != null) {
                    // 转化为字符串
                    String oldDataString = new String(oldData.getData());
                    System.out.println(oldDataString);
                    // 从信息表中找到旧数据的索引
                    int i = infoList.indexOf(oldDataString);
                    // 如果新数据为空
                    if (data == null) {
                        infoList.remove(i);
                    } else {
                        String newDataString = new String(data.getData());
                        System.out.println(newDataString);
                        infoList.set(i, new String(data.getData()));
                    }
                } else {
                    if (data != null) {
                        infoList.add(new String(data.getData()));
                    }
                }
            }
        });
        
        // 3. 开启监听
        curatorCache.start();
    }
    
    /**
     * 发送时间查询的请求
     */
    public void sendGetTimeRequest() throws IOException {
        // 目标服务器地址
        Random random = new Random();
        int i = random.nextInt(infoList.size());
        
        String ipPort = infoList.get(i);
        while (ipPort == null || "".equals(ipPort)) {
            i = random.nextInt(infoList.size());
            ipPort = infoList.get(i);
        }
        String[] ipAndPort = ipPort.split(":");
        // IP
        String host = ipAndPort[0];
        // port
        Integer port = Integer.parseInt(ipAndPort[1]);
        
        // 建立 socket 连接
        Socket socket = new Socket(host, port);
        OutputStream outputStream = socket.getOutputStream();
        InputStream inputStream = socket.getInputStream();
        
        // 发送请求数据
        outputStream.write("getTime".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
        
        // 接收响应数据
        byte[] bytes = new byte[1024];
        inputStream.read(bytes);
        System.out.println("Client 接收到 Server 主机：" + host + " : " + port + " 返回的结果：" + new String(bytes, StandardCharsets.UTF_8));
        
        inputStream.close();
        outputStream.close();
        socket.close();
    }
    
    public static void main(String[] args) throws Exception {
        Client client = new Client();
        // 监听逻辑
        client.connectToZk();
        while (true) {
            // 发送请求
            client.sendGetTimeRequest();
            // 每隔5秒发送一次
            Thread.sleep(2000);
        }
    }
    
}
