package com.tlh.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.tlh.gmall.realtime.bean.SkuInfo
import com.tlh.gmall.realtime.util.{KafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Comment 用户维度数据
 * @Author: tlh
 * @Date 2020/7/28 20:33
 * @Version 1.0
 */
object DimSkuInfoApp {

    def main(args: Array[String]): Unit = {
        /**
         *1.mysql --maxwell-->
         *2.ods数据  --BaseDbMaxwellApp-->
         *3.topic ODS_SKU_INFO --DimskuInfoApp-->
         *4.保存到hbase(phoenix) -->dwd...
         */
        //1.环境初始化
        val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_sku_info_app")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        //2.数据源信息
        val groupId = "dim_sku_info_app"
        val topic = "ODS_SKU_INFO"
        //3.偏移量读取
        val offsetMap = OffsetManagerUtil.getOffset(topic, groupId)
        //4.读取数据
        var skuInfoInputDStream : InputDStream[ConsumerRecord[String,String]] = null
        if (offsetMap != null && offsetMap.size > 0){
            skuInfoInputDStream = KafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
        } else {
            skuInfoInputDStream = KafkaUtil.getKafkaStream(topic, ssc, groupId)
        }
        //5.数据偏移量结束点获取 数据转换
        var offsetRanges : Array[OffsetRange] = null
        val skuInfoGetOffsetInputDStream = skuInfoInputDStream.transform { rdd =>
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }
        //数据实体转换
        val skuInfoDS = skuInfoGetOffsetInputDStream.map { record =>
            val skuInfoJson = record.value()
            val skuInfo = JSON.parseObject(skuInfoJson, classOf[SkuInfo])
            skuInfo
        }
        skuInfoDS.foreachRDD{rdd=>
            //6.写入phonenix  支持幂等性(主键相同覆盖)
            rdd.saveToPhoenix("GMALL_SKU_INFO",Seq("ID","SPU_ID","PRICE","SKU_NAME","SKU_DESC","WEIGHT","TM_ID","CATEGORY3_ID","SKU_DEFAULT_IMG","CREATE_TIME"),new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
            //7.缓存偏移量
            OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
        }
        //8.环境启动,等待
        ssc.start()
        ssc.awaitTermination()

    }

}
