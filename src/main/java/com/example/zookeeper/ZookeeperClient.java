package com.example.zookeeper;

import com.example.zookeeper.utils.PathUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;

/**
 * 默认zk客户端
 * <p>
 * Copyright: Copyright (c) 2019/6/17 16:33
 * <p>
 * Company: DataSense
 * <p>
 *
 * @author Jason chenjb1209@163.com
 * @version 1.0
 * @on
 */
@Slf4j
public class ZookeeperClient {

    private static final byte[] EMPTY_BYTE = new byte[0];

    private static final String UTF_8 = "UTF-8";

    private static final int SESSION_TIMEOUT = 30000;

    private ZooKeeper zooKeeper;

    private ZookeeperClient() {
    }

    /**
     * 创建会话
     *
     * @param connectString  指服务器列表（由“,”分开的host:port字符串组成，每一个都代表一台zookeeper服务器，例如：192.168.1.1:2181
     *                       ,192.168.1.2:2181,192.168.1.3:2181,这样就为客户端指定了三台服务器地址，）
     * @param sessionTimeout Zookeeper客户端和服务器端会话的建立是一个异步的过程，也就是说，方法在处理完客户端初始化方法后立即返回，在大多数情况下
     *                       此时并没有正真建立好一个可用的会话，在会话生命周期中处于"CONNECTING"的状态，当会话正真创建完毕后，Zookeeper服务器会向会话对应的客户端
     *                       发送一个时间通知，以告知客户端，客户端只有在获取这个通知后才算正真建立了会话
     */
    public static ZookeeperClient createSession(String connectString, int sessionTimeout) {
        ZookeeperClient zookeeperClient = new ZookeeperClient();
        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            zookeeperClient.zooKeeper = new ZooKeeper(connectString, sessionTimeout, (event) -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    //解除阻塞
                    countDownLatch.countDown();
                    log.info("ZookeeperClient 会话创建完成......");
                }
            });
            initializeChroot(connectString);
            countDownLatch.await();
        } catch (Exception e) {
            log.error("", e);
            throw new RuntimeException(e);
        }
        return zookeeperClient;
    }


    /**
     * 创建可复用的会话
     * 第一次连接上Zookeeper服务器，通过调用Zookeeper实例的以下两个方法 即可获取到会话ID 和会话 秘钥
     * long getSessionId()
     * byte[] getSessionPasswd()
     *
     * @param connectString
     * @param sessionTimeout
     * @param watcher
     * @param sessionId      会话ID
     * @param sessionPasswd  会话秘钥
     *                       <p>
     *                       sessionId 和 sessionPasswd这两个参数可以唯一确定一个会话，客户端使用这两个参数可以实现客户端会话复用，从而达到回复会话的效果
     */
    public void createSession(String connectString, int sessionTimeout, Watcher watcher, long sessionId, byte[]
            sessionPasswd) {

    }


    /**
     * 创建持久节点，父节点不存在的话自动创建
     * zookeeper不支持递归创建
     *
     * @param nodePath 节点路径
     * @return
     */
    public String createPersistent(String nodePath) {

        return create(nodePath, null, CreateMode.PERSISTENT);
    }

    /**
     * 创建持久顺序节点，父节点不存在的话自动创建
     * zookeeper不支持递归创建
     *
     * @param nodePath 节点路径
     * @return
     */
    public String createPersistentSequential(String nodePath) {
        return create(nodePath, null, CreateMode.PERSISTENT_SEQUENTIAL);
    }


    /**
     * 创建持久节点，父节点不存在的话自动创建
     *
     * @param nodePath
     * @param nodeValue
     * @return
     */
    public String createPersistent(String nodePath, String nodeValue) {
        return create(nodePath, nodeValue, CreateMode.PERSISTENT);
    }

    /**
     * 创建持久顺序节点，父节点不存在的话自动创建
     *
     * @param nodePath
     * @param nodeValue
     * @return
     */
    public String createPersistentSequential(String nodePath, String nodeValue) {
        return create(nodePath, nodeValue, CreateMode.PERSISTENT_SEQUENTIAL);
    }


    /**
     * 创建临时节点，父节点不存在的话自动创建
     * zookeeper不支持递归创建
     *
     * @param nodePath
     * @return
     */
    public String createEphemeral(String nodePath) {
        return create(nodePath, null, CreateMode.EPHEMERAL);
    }

    /**
     * 创建临时节点，父节点不存在的话自动创建
     *
     * @param nodePath
     * @param nodeValue
     * @return
     */
    public String createEphemeral(String nodePath, String nodeValue) {
        return create(nodePath, nodeValue, CreateMode.EPHEMERAL);
    }

    /**
     * 创建临时顺序节点，父节点不存在的话自动创建
     * zookeeper不支持递归创建
     *
     * @param nodePath
     * @return
     */
    public String createEphemeralSequential(String nodePath) {
        return create(nodePath, null, CreateMode.EPHEMERAL_SEQUENTIAL);
    }


    /**
     * 创建临时顺序节点，父节点不存在的话自动创建
     *
     * @param nodePath
     * @param nodeValue
     * @return
     */
    public String createEphemeralSequential(String nodePath, String nodeValue) {
        return create(nodePath, nodeValue, CreateMode.EPHEMERAL_SEQUENTIAL);
    }


    /**
     * 递归创建节点 zookeeper 创建节点 路径必须是绝对路径 以"/开头的"
     *
     * @param nodePath
     * @param nodeValue
     * @param createMode
     * @return
     */
    public String create(String nodePath, String nodeValue, CreateMode createMode) {
        if (StringUtils.isBlank(nodePath)) {
            throw new RuntimeException("Missing value for nodePath");
        }

        byte[] data = EMPTY_BYTE;
        if (StringUtils.isNotBlank(nodeValue)) {
            try {
                data = nodeValue.getBytes(UTF_8);
            } catch (UnsupportedEncodingException e) {
                log.error("", e);
                throw new RuntimeException(e);
            }
        }

        String[] path = PathUtils.pathSplit(nodePath);

        String createNodePath = "";

        try {
            for (int i = 0; i < path.length; i++) {
                String nodeSplit = "/";
                if (i == 0) {
                    createNodePath = StringUtils.join(nodeSplit, path[i]);
                } else {
                    createNodePath = StringUtils.join(createNodePath, nodeSplit, path[i]);
                }
                //判断节点是否存在
                if (zooKeeper.exists(createNodePath, false) == null) {
                    log.info("create node {} ", createNodePath);
                    //其余节点相对于最后一个子节点都是父节点，
                    //父节点只能创建持久节点
                    if (i < path.length - 1) {
                        try {
                            zooKeeper.create(createNodePath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        } catch (KeeperException.NodeExistsException e) {
                            log.info("父节点{}已经存在, 不用创建", createNodePath);
                        }
                        continue;
                    }

                    //创建最后一个节点，最后一个节点才可能是临时节点
                    try {
                        return zooKeeper.create(createNodePath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
                    } catch (KeeperException.NodeExistsException e) {
                        log.info("子节点{}已经存在, 不用创建", createNodePath);
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            log.error("", e);
        }
        return null;
    }

    /**
     * 初始化chroot目录
     *
     * @param url
     */
    public static void initializeChroot(String url) {
        int chrootIndex = url.indexOf("/");
        if (chrootIndex == -1) {
            return;
        }
        String chrootPaht = url.substring(chrootIndex);
        url = url.substring(0, chrootIndex);

        if (chrootPaht.length() != 1) {
            CountDownLatch countDownLatch = new CountDownLatch(1);

            try {
                ZooKeeper zooKeeper = new ZooKeeper(url, SESSION_TIMEOUT, (event) -> {
                    if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                        //唤醒阻塞
                        countDownLatch.countDown();
                    }
                });
                countDownLatch.await();
                ZookeeperClient zookeeperClient = new ZookeeperClient();
                zookeeperClient.zooKeeper = zooKeeper;
                zookeeperClient.createPersistent(chrootPaht);
                zooKeeper.close();
            } catch (Exception e) {
                log.error("", e);
                throw new RuntimeException(e);
            }

        }


    }

}