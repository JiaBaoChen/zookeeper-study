package com.example.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * <p>
 * Copyright: Copyright (c) 2019/6/19 18:03
 * <p>
 * Company: DataSense
 * <p>
 *
 * @author Jason chenjb1209@163.com
 * @version 1.0
 * @on
 */
public class WatcherDemo implements Watcher{

    @Override
    public void process(WatchedEvent event) {

        if(event.getType() == Event.EventType.NodeChildrenChanged){

        }

    }
}