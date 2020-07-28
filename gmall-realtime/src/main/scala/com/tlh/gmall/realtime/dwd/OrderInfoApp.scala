package com.tlh.gmall.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.tlh.gmall.realtime.bean.{OrderInfo, UserState}
import com.tlh.gmall.realtime.util.{EsUtil, KafkaSinkUtil, KafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/26 22:19
 * @Version 1.0
 */
object OrderInfoApp {
    def main(args: Array[String]): Unit = {
        // 加载流 //手动偏移量
        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dwd_order_info_app")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val groupId = "dwd_order_info_group"
        val topic = "ODS_ORDER_INFO";


        //1.从redis中读取偏移量 （启动执行一次）
        val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

        //2.把偏移量传递给kafka ，加载数据流（启动执行一次）
        var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetMapForKafka != null && offsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
            recordInputDstream = KafkaUtil.getKafkaStream(topic, ssc, offsetMapForKafka, groupId)
        } else {
            recordInputDstream = KafkaUtil.getKafkaStream(topic, ssc, groupId)
        }


        //3.从流中获得本批次的 偏移量结束点（每批次执行一次）
        var offsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
        val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform { rdd => //周期性在driver中执行
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }


        // 1 提取数据 2 分topic
        val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map { record =>
            val jsonString: String = record.value()
            //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
            val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
            val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
            orderInfo.create_date = createTimeArr(0)
            orderInfo.create_hour = createTimeArr(1).split(":")(0)
            orderInfo
        }
        //map-> filter -> store
        // 按照周期+分区 组成大sql查询
        // select xxx from user_state where user_id in (xxx,xxx,x,xxx,xx,xx)
        val orderInfoWithFlagDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>

            val orderInfoList: List[OrderInfo] = orderInfoItr.toList
            if (orderInfoList != null && orderInfoList.size > 0) {
                val userIdList: List[Long] = orderInfoList.map(orderInfo => orderInfo.user_id)
                //1,2,3    in ('1','2','3')"
                val sql = "select  USER_ID,IF_CONSUMED from  USER_STATE where USER_ID in ('" + userIdList.mkString("','") + "')"
                val ifConsumedList: List[JSONObject] = PhoenixUtil.queryList(sql)
                // List=>list[(k,v)]=> map
                val ifConsumedMap: Map[String, String] = ifConsumedList.map(jsonObj => (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED"))).toMap
                for (orderInfo <- orderInfoList) {
                    //for (jsonObj <- ifConsumedList) {}  //
                    val ifConsumed: String = ifConsumedMap.getOrElse(orderInfo.user_id.toString, "0")
                    if (ifConsumed == "1") { //消费过的用户
                        orderInfo.if_first_order = "0" //不是首单
                    } else {
                        orderInfo.if_first_order = "1" //否则是首单
                    }
                }
            }
            orderInfoList.toIterator
        }
        // 问题：
        //同一批次 同一个用户两次下单 如何解决 只保证第一笔订单为首单 其他订单不能为首单
        //矫正
        // 1  想办法让相同user_id的订单在一个分区中， 这样只要处理 mapPartition中的list就行了
        //--》 上游写入kafka 时 用userId 当分区键

        //2  groupbykey  按照某一个键值进行分组
        //   每组  取第一笔订单设为首单    非第一笔 设为 非首单   ，前提是：已经被全部设为首单
        val orderInfoGroupByUserIdDstream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithFlagDstream.map(orderInfo => (orderInfo.user_id, orderInfo)).groupByKey()
        val orderInfoRealWithFirstFlagDstream: DStream[OrderInfo] = orderInfoGroupByUserIdDstream.flatMap { case (userId, orderInfoItr) =>
            val orderList: List[OrderInfo] = orderInfoItr.toList
            if (orderList != null && orderList.size > 0) {
                val orderInfoAny: OrderInfo = orderList(0) // 随便取一笔订单 用于检验是否被打了首单标志
                //需要修正的两个条件 1 一个批次内做了2笔以上订单  2 其中有首单  //需要修正
                if (orderList.size >= 2 && orderInfoAny.if_first_order == "1") {
                    // 排序
                    val sortedList: List[OrderInfo] = orderList.sortWith((orderInfo1, orderInfo2) => orderInfo1.create_time < orderInfo2.create_time)
                    for (i <- 1 to sortedList.size - 1) { //不是本批次第一笔订单  要还原成非首单
                        val orderInfoNotFirstThisBatch: OrderInfo = sortedList(i)
                        orderInfoNotFirstThisBatch.if_first_order = "0"
                    }
                    sortedList
                } else {
                    orderList
                }
            } else {
                orderList
            }
        }

        orderInfoRealWithFirstFlagDstream.print(1000)

        //写入操作
        orderInfoRealWithFirstFlagDstream.foreachRDD { rdd =>
            val userStateRDD: RDD[UserState] = rdd.map(orderInfo => UserState(orderInfo.user_id.toString, "1"))
            // 1.更新用户状态
            userStateRDD.saveToPhoenix("USER_STATE",
                Seq("USER_ID", "IF_CONSUMED"),
                new Configuration,
                Some("hadoop102,hadoop103,hadoop104:2181"))
            rdd.foreachPartition { orderInfoItr =>
                val orderInfoList: List[(OrderInfo, String)] = orderInfoItr.toList.map(orderInfo => (orderInfo, orderInfo.id.toString))
                val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
                // 2.存储olap(es)-->用户分析
                EsUtil.bulkSave(orderInfoList, "gmall0213_order_info_" + dateStr)

                // 3.推kafka进入下一层处理   可选  主题： DWD_ORDER_INFO
                for ((orderInfo, id) <- orderInfoList) {  //fastjson 要把scala对象包括caseclass转json字符串 需要加入,new SerializeConfig(true)
                    //json4s scala专用工具
                   KafkaSinkUtil.send("DWD_ORDER_INFO", id, JSON.toJSONString(orderInfo,new SerializeConfig(true)))
                }

            }
            //4.提交偏移量 //driver中执行
            OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
        }
        ssc.start()
        ssc.awaitTermination()
    }

}
