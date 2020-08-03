package com.tlh.gmall.publisher.service;

import com.tlh.gmall.publisher.bean.HourAmount;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/31 20:04
 * @Version 1.0
 */
public interface OrderService {
    //查询订单总额
    public BigDecimal getOrderTotalAmount(String dt);

    //查询分时金额
    public Map getOrderHourAmount(String dt);

}
