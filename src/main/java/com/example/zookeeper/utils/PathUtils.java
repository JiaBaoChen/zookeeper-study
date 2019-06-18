package com.example.zookeeper.utils;

import java.util.Objects;

/**
 * 路径截取
 * 首先检查，创建的节点必须以"/"开头
 * <p>
 * Copyright: Copyright (c) 2019/6/18 11:11
 * <p>
 * Company: DataSense
 * <p>
 *
 * @author Jason chenjb1209@163.com
 * @version 1.0
 * @on
 */
public class PathUtils {


    public static final String PATH_SPLIT = "/";

    public static String[] pathSplit(String nodePath) {
        Objects.requireNonNull(nodePath, "node path can not be null");

        if (nodePath.indexOf(nodePath) != 0) {
            throw new RuntimeException("node path must be start whih /");
        }

        String[] path = nodePath.split(PATH_SPLIT);

        String[] result = new String[path.length - 1];
        for (int i = 1; i < path.length; i++) {
            result[i - 1] = path[i];
        }

        return result;

    }
}   