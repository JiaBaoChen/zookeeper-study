package com.example.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * FileName: ZkDistributeLock
 * Author:   Jason chenjb1209@163.com
 * Date:     2019/6/7 19:46
 * Description: 分布式锁
 */
public class ZkDistributeLock implements Lock {

    private String lockPath;
    private ZkClient zkClient;
    //ip:port
    private String zkUrl;

    public ZkDistributeLock(String lockPath) {
        if (lockPath == null || lockPath.trim().equals("")) {
            throw new IllegalArgumentException("lockPath不能为空");
        }
        this.lockPath = lockPath;
        zkClient = new ZkClient(zkUrl);
        zkClient.setZkSerializer(new MyZkSerializer());
    }

    /**
     * 尝试获取锁
     *
     * @return
     */
    @Override
    public boolean tryLock() {
        try {
            zkClient.createEphemeral(lockPath);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * 获取锁
     */
    @Override
    public void lock() {
        //尝试获取锁，如果失败，阻塞等待
        if (!tryLock()) {
            waitForLock();
            lock();
        }
    }

    //等待获取锁的时候，还需监听锁什么时候释放
    private void waitForLock() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        IZkDataListener iZkDataListener = new IZkDataListener() {

            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
            }

            //有人释放了锁，激活监听，唤醒阻塞线程
            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                countDownLatch.countDown();
            }
        };

        //监听，获取一个watch
        zkClient.subscribeDataChanges(lockPath, iZkDataListener);
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //释放掉watch
        zkClient.unsubscribeDataChanges(lockPath, iZkDataListener);
    }

    //释放锁
    @Override
    public void unlock() {
        zkClient.delete(lockPath);

    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }


    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }


    @Override
    public Condition newCondition() {
        return null;
    }
}