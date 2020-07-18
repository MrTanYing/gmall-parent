package com.tlh.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.tlh.gmall.realtime.util.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
            val ts = startupJSONObj.getLong("ts")
            startupJSONObj
        }
        startLogInfoDStream.print(100)

        ssc.start()
        ssc.awaitTermination()



    }

}
