package com.tlh.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.tlh.gmall.realtime.bean.DauInfo
import com.tlh.gmall.realtime.util.{EsUtil, KafkaUtil, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @Comment DAU 日活
 * @Author: tlh
 * @Date 2020/7/17 22:14
 * @Version 1.0
 */
object DauApp {

    def main(args: Array[String]): Unit = {
        //setMaster("local[4]")  设置为kafka 分区数一致
        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val groupId = "GMALL_DAU_CONSUMER"
        val topic = "GMALL_START"

        //保证精确一致性消费(相对的)
        //手动提交偏移量+幂等性(比较难保证)
        /**
         * 手动提交偏移量
         * 存于redis,读取偏移量用于kafka消费,消费完毕存于redis,循环操作
         */
        //1.从redis读取偏移量
        val offsetMapForKafka = OffsetManagerUtil.getOffset(topic, groupId)
        //2.kafka数据流获取
        var startupInputDstream : InputDStream[ConsumerRecord[String,String]] =null
        if(offsetMapForKafka!=null&&offsetMapForKafka.size>0){
            startupInputDstream = KafkaUtil.getKafkaStream(topic, ssc,offsetMapForKafka,groupId)
        }else{
            startupInputDstream = KafkaUtil.getKafkaStream(topic,ssc,groupId)
        }

        //3.从流中获得本次消费偏移量的结束点

        var offsetRanges: Array[OffsetRange] = null

        startupInputDstream.transform { record =>
            println("1111111")
            offsetRanges = record.asInstanceOf[HasOffsetRanges].offsetRanges
            record
        }
        println("2222222")
        //把json转成jsonobj
        val startLogInfoDStream: DStream[JSONObject] = startupInputDstream.map { record =>
            val startupJson: String = record.value()
            val startupJSONObj: JSONObject = JSON.parseObject(startupJson)
            startupJSONObj
        }

        val startLogInfoFilteredDStream: DStream[JSONObject] = startLogInfoDStream.mapPartitions { startLogInfoItr =>
            val startLogBeforeList: List[JSONObject] = startLogInfoItr.toList
            println("过滤前: " + startLogBeforeList.size)
            val dauLogInfoList = new ListBuffer[JSONObject]
            val jedis: Jedis = RedisUtil.getJedisClient

            for (startupJSONObj <- startLogBeforeList) {
                val mid = startupJSONObj.getJSONObject("common").getString("mid")
                val ts: java.lang.Long = startupJSONObj.getLong("ts")
                val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
                val dauKey = "dau:" + dt
                print("key: "+dauKey + "||mid"+mid)
                val ifFirst: java.lang.Long = jedis.sadd(dauKey,mid)
                if (ifFirst == 1L) {
                    dauLogInfoList.append(startupJSONObj)
                }
            }
            jedis.close()
            println("过滤后: " +dauLogInfoList.size)
            dauLogInfoList.toIterator
        }

        //写入到ES中
        startLogInfoFilteredDStream.foreachRDD{rdd=>

            // rdd.foreach(jsonObj=>println(jsonObj)) // 写入数据库的操作
            rdd.foreachPartition{jsonObjItr=>
                val jsonObjList: List[JSONObject] = jsonObjItr.toList
                val formattor = new SimpleDateFormat("yyyy-MM-dd HH:mm")
                val dauWithIdList: List[(DauInfo, String)] = jsonObjList.map { jsonObj =>

                    val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")

                    //获取日期 、小时、分钟
                    val ts: lang.Long = jsonObj.getLong("ts")
                    val dateTimeString: String = formattor.format(new Date(ts))
                    val dateTimeArr: Array[String] = dateTimeString.split(" ")
                    val dt: String = dateTimeArr(0)
                    val time: String = dateTimeArr(1)
                    val timeArr: Array[String] = time.split(":")
                    val hr: String = timeArr(0)
                    val mi: String = timeArr(1)

                    val dauInfo = DauInfo(commonJsonObj.getString("mid"),
                        commonJsonObj.getString("uid"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("vc"),
                        dt, hr, mi, ts
                    )
                    (dauInfo, dauInfo.mid) //日活表（按天切分索引)
                }
                val today= new SimpleDateFormat("yyyyMMdd").format(new Date())
                EsUtil.bulkSave(dauWithIdList,"gmall_dau_info_"+today)

            }

            if(offsetRanges!=null&&offsetRanges.length>0)
            OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)// 要在driver中执行 周期性 每批执行一次

        }

        ssc.start()
        ssc.awaitTermination()
    }

}
