package com.tlh.gmall.realtime.bean

import java.util.Date

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/28 23:49
 * @Version 1.0
 */
case class SkuInfo(
                    id:Long,
                    spu_id:Long,
                    price:Double ,
                    sku_name:String,
                    sku_desc:String,
                    weight:Double,
                    tm_id:Long,
                    category3_id:Long,
                    sku_default_img:String,
                    create_time:Date
                  )
