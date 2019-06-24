package com.example.zookeeper;

import com.sun.org.apache.xpath.internal.SourceTree;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDate;
import java.util.List;
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
        ZookeeperClient zookeeperClient = ZookeeperClient.createSession("192.168.184.129:2181", 30000);
        String path = StringUtils.join("/ticketNo/", LocalDate.now().toString().replaceAll("-", ""), "/tempNode_");
        String result = zookeeperClient.createEphemeralSequential(path);
        System.out.println(result);
    }

    @Test
    public void test02() throws InterruptedException {
        ZookeeperClient zookeeperClient = ZookeeperClient.createSession("192.168.184.129:2181", 30000);
        String path = StringUtils.join("/ticketNo/", LocalDate.now().toString().replaceAll("-", ""), "/tempNode_");
        CountDownLatch countDownLatch = new CountDownLatch(1);
        for (int i = 0; i < 200; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    String result = zookeeperClient.createEphemeralSequential(path);
                    System.out.println("=======" + result);
                }
            }).start();
        }
        countDownLatch.countDown();

        Thread.sleep(5000);
    }

    @Test
    public void test04() {

    }


    @Test
    public void test03() {
        ZookeeperClient zookeeperClient = ZookeeperClient.createSession("192.168.184.129:2181", 30000);

//        zookeeperClient.getChildren("/ticketNo/20190619",false).forEach(s -> {
//            System.out.println(s);
//        });
        zookeeperClient.getChildren("/ticketNo/20190619", event -> {
//           if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged){

            System.out.println("/ticketNo===有变化");
            zookeeperClient.getChildren("/ticketNo/20190619", false).forEach(s -> {
                System.out.println("变更子节点==" + s);
            });
//           }
        }).forEach(s -> {
            System.out.println("子节点==" + s);
        });

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void test05() {
        ZookeeperClient zookeeperClient = ZookeeperClient.createSession("192.168.184.129:2181", 30000);
        zookeeperClient.getChildren("/ticketNo", event -> {
        }, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                System.out.println("rc==" + rc);
                System.out.println("path==" + path);
                System.out.println("ctx==" + ctx);
                children.forEach(s -> {
                    System.out.println("子节点==" + s);
                });

            }
        }, null);

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test06() {
        ZookeeperClient zookeeperClient = ZookeeperClient.createSession("192.168.184.129:2181", 30000);
        String path = StringUtils.join("/ticketNo/", LocalDate.now().toString().replaceAll("-", ""), "/tempNode_");
        Stat stat = zookeeperClient.exists(path, null);
        if (stat != null) {
            System.out.println(stat.toString());
        }
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test07() {
        ZookeeperClient zookeeperClient = ZookeeperClient.createSession("192.168.184.129:2181", 30000);
        zookeeperClient.setData("/ticketNo","hello2".getBytes(),2);
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test08() {
        ZookeeperClient zookeeperClient = ZookeeperClient.createSession("192.168.184.129:2181", 30000);
        byte[] b = zookeeperClient.getData("/ticketNo",event -> {});
        for (byte b1 : b) {
            System.out.println(b1);
        }
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test11(){

        CountDownLatch countDownLatch = new CountDownLatch(1);

        for (int i = 0; i <10 ; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        countDownLatch.await();
                        System.out.println(ZookeeperClient.createSession("/ticketNo",30000));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        countDownLatch.countDown();
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test12(){
        String s = "0000012345";
        System.out.println(s.substring(-1,8));
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
