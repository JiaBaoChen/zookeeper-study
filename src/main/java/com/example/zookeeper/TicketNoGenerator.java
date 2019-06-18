package com.example.zookeeper;

import java.time.LocalDate;

/**
 * 单号生成器
 * <p>
 * Copyright: Copyright (c) 2019/6/17 15:37
 * <p>
 * Company: DataSense
 * <p>
 *
 * @author Jason chenjb1209@163.com
 * @version 1.0
 * @on
 */
public interface TicketNoGenerator {

    /**
     * 方案一 前缀列+日期（年月日）+自增列
     *
     * @param prefixNo 前缀列
     * @param date     日期
     * @param incrNum  自增列
     * @return
     */
    public String getTicketNo(String prefixNo, LocalDate date, int incrNum);

}
