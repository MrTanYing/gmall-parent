package com.tlh.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.tlh.gmall.realtime.bean.BaseCategory3
import com.tlh.gmall.realtime.util.{KafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * @Comment 商品三级分类维度数据
 * @Author: tlh
 * @Date 2020/7/29 21:48
 * @Version 1.0
 */
object DimBaseCategory3App {

    def main(args: Array[String]): Unit = {
        /**
         *1.mysql --maxwell-->
         *2.ods数据  --BaseDbMaxwellApp-->
         *3.topic ODS_SKU_INFO --DimBaseCategory3App-->
         *4.保存到hbase(phoenix) -->dwd...
         */
        //1.环境初始化
        val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_base_category3_app")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        //2.数据源信息
        val groupId = "dim_base_category3_app"
        val topic = "ODS_BASE_CATEGORY3"
        //3.偏移量读取
        val offsetMap = OffsetManagerUtil.getOffset(topic, groupId)
        //4.读取数据
        var baseCategory3InputDStream : InputDStream[ConsumerRecord[String,String]] = null
        if (offsetMap != null && offsetMap.size > 0){
            baseCategory3InputDStream = KafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
        } else {
            baseCategory3InputDStream = KafkaUtil.getKafkaStream(topic, ssc, groupId)
        }
        //5.数据偏移量结束点获取 数据转换
        var offsetRanges : Array[OffsetRange] = null
        val skuInfoGetOffsetInputDStream = baseCategory3InputDStream.transform { rdd =>
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }
        //数据实体转换
        val skuInfoDS = skuInfoGetOffsetInputDStream.map { record =>
            val skuInfoJson = record.value()
            val baseCategory3 = JSON.parseObject(skuInfoJson, classOf[BaseCategory3])
            baseCategory3
        }
        skuInfoDS.foreachRDD{rdd=>
            //6.写入phonenix  支持幂等性(主键相同覆盖)
            rdd.saveToPhoenix("GMALL_BASE_CATEGORY3",Seq("ID","NAME"),new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
            //7.缓存偏移量
            OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
        }
        //8.环境启动,等待
        ssc.start()
        ssc.awaitTermination()
    }

}
