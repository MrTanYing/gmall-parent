package com.tlh.gmall.publisher.service;

import java.util.Map;

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/24 10:38
 * @Version 1.0
 */
public interface DauService {
    //求某日日活总值
    public Long  getDauTotal(String date);

    //求某日日活的分时值
    public Map getDauHourCount(String date);
}
