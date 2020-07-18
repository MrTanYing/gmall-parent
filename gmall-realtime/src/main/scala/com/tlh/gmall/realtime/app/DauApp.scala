package com.tlh.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.tlh.gmall.realtime.util.{KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
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
        val startupInputDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(topic, ssc)

        val startLogInfoDStream: DStream[JSONObject] = startupInputDstream.map { record =>
            val startupJson: String = record.value()
            val startupJSONObj: JSONObject = JSON.parseObject(startupJson)
            startupJSONObj
        }

        startLogInfoDStream.print(2)

        val dauLoginfoDstream: DStream[JSONObject] = startLogInfoDStream.transform{ rdd =>
            println("前：" +  rdd.count())
            val logInfoRdd: RDD[JSONObject] = rdd.mapPartitions { startLogInfoItr =>
                val jedis: Jedis = RedisUtil.getJedisClient
                val dauLogInfoList = new ListBuffer[JSONObject]
                val startLogList: List[JSONObject] = startLogInfoItr.toList

                for (startupJSONObj <- startLogList) {
                    val ts: java.lang.Long = startupJSONObj.getLong("ts")
                    val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
                    val dauKey = "dau:" + dt
                    val ifFirst: java.lang.Long = jedis.sadd(dauKey, startupJSONObj.getJSONObject("common").getString("mid"))
                    if (ifFirst == 1L) {
                        dauLogInfoList += startupJSONObj
                    }
                }
                jedis.close()
                dauLogInfoList.toIterator
            }
            println("后：" + logInfoRdd.count())
            logInfoRdd
        }

        ssc.start()
        ssc.awaitTermination()
    }

}
