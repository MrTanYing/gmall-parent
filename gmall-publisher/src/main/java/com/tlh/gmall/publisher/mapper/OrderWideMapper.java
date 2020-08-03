package com.tlh.gmall.publisher.mapper;

import com.tlh.gmall.publisher.bean.HourAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/31 19:51
 * @Version 1.0
 */
public interface OrderWideMapper {
    //查询订单总额
    public BigDecimal getOrderTotalAmount(String dt);

    //查询分时金额
    public List<HourAmount> getOrderHourAmount(String dt);
}
