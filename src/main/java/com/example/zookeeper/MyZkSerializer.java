package com.example.zookeeper;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.io.UnsupportedEncodingException;

/**
 * Copyright (C), 2019, 深数意识有限公司
 * FileName: MyZkSerializer
 * Author:   Jason chenjb1209@163.com
 * Date:     2019/4/11 11:13
 * Description:
 */
public class MyZkSerializer implements ZkSerializer {


    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        String d = (String) data;
        try {
            return d.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
}