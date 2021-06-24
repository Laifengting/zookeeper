package com.lft.zk.service;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeService extends Thread {
    
    private Integer port;
    
    public TimeService(Integer port) {
        this.port = port;
        
    }
    
    @Override
    public void run() {
        // 通过 socket 与 客户端进行交流。
        // 启动 serverSocket 监听请求
        try {
            // 指定监听的端口
            ServerSocket serverSocket = new ServerSocket(port);
            
            // 保证服务端一直运行
            while (true) {
                Socket socket = serverSocket.accept();
                // 不关心 客户端发送内容， server 只老虎发送一个时间值
                OutputStream outputStream = socket.getOutputStream();
                outputStream.write(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()).getBytes());
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
