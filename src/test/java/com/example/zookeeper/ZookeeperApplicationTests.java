package com.example.zookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDate;
import java.util.concurrent.CountDownLatch;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ZookeeperApplicationTests {

    @Test
    public void contextLoads() {
        System.out.println("=");
    }

    @Test
    public void testZookeeperClient() {
//        CountDownLatch countDownLatch = new CountDownLatch(20);
        for (int i = 0; i < 79; i++) {
            ZookeeperClient zookeeperClient = ZookeeperClient.createSession("192.168.184.129:2181", 30000);
            String path = StringUtils.join("/ticketNo/", LocalDate.now().toString().replaceAll("-",""), "/tempNode_");
            String result = zookeeperClient.createEphemeralSequential(path);
            System.out.println(result);
        }
    }

    @Test
    public void test01() {
        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            ZooKeeper zooKeeper = new ZooKeeper("192.168.184.129:2181", 30000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    countDownLatch.countDown();
                    System.out.println("zookeeper init complete");
                }
            });
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
