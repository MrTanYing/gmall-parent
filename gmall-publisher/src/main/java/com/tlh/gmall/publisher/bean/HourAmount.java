package com.tlh.gmall.publisher.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/31 19:54
 * @Version 1.0
 */
@Data
public class HourAmount {

    String hr;
    BigDecimal orderAmount;
}
