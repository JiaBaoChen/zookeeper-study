package com.example.zookeeper;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

import java.util.List;

/**
 * Copyright (C), 2019, 深数意识有限公司
 * FileName: ZkClientDemo
 * Author:   Jason chenjb1209@163.com
 * Date:     2019/4/9 16:50
 * Description:
 */
public class ZkClientDemo {

    public static void main(String[] args) {
        ZkClient zkClient = new ZkClient("192.168.184.129:2181");
        zkClient.create("/zkclient", "date", CreateMode.PERSISTENT_SEQUENTIAL);
        zkClient.setZkSerializer(new MyZkSerializer());
        zkClient.subscribeChildChanges("/zkclient", new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                currentChilds.forEach(child -> {
                    System.out.println("/zkclient子节点" + child);
                });
            }
        });

        zkClient.subscribeDataChanges("/zkclient", new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                System.out.println("节点发生改变==" + dataPath + "==data==" + data);
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println("删除节点==" + dataPath);
            }
        });

        zkClient.subscribeStateChanges(new IZkStateListener() {
            @Override
            public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
            }

            @Override
            public void handleNewSession() throws Exception {
                System.out.println("处理新的会话" );
            }

            @Override
            public void handleSessionEstablishmentError(Throwable error) throws Exception {
            }
        });


        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}