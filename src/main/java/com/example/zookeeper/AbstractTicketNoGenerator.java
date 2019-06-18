package com.example.zookeeper;

import java.time.LocalDate;

/**
 * 默认实现,基于zookeeper特点来实现
 * <p>
 * Copyright: Copyright (c) 2019/6/17 16:06
 * <p>
 * Company: DataSense
 * <p>
 *
 * @author Jason chenjb1209@163.com
 * @version 1.0
 * @on
 */
public abstract class AbstractTicketNoGenerator implements TicketNoGenerator{


    @Override
    public String getTicketNo(String prefixNo, LocalDate date, int incrNum) {
        return null;
    }
}