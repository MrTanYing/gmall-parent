package com.tlh.gmall.publisher.service.impl;

import com.tlh.gmall.publisher.bean.HourAmount;
import com.tlh.gmall.publisher.mapper.OrderWideMapper;
import com.tlh.gmall.publisher.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/31 20:05
 * @Version 1.0
 */
@Service
public class OrderServiceImpl  implements OrderService {

    @Autowired
    private OrderWideMapper orderWideMapper;


    @Override
    public BigDecimal getOrderTotalAmount(String dt) {
        return orderWideMapper.getOrderTotalAmount(dt);
    }

    @Override
    public Map getOrderHourAmount(String dt) {
        List<HourAmount> hourAmountList = orderWideMapper.getOrderHourAmount(dt);
        Map hourAmountMap=new HashMap();
        for (HourAmount hourAmount : hourAmountList) {
            hourAmountMap.put(hourAmount.getHr(),hourAmount.getOrderAmount());
        }
        return hourAmountMap;
    }
}
