package com.tlh.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.JavaConverters._

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/18 15:22
 * @Version 1.0
 */
object OffsetManagerUtil {
    def getOffset(topic:String,consumerGroupId:String) = {
        //redis hash  主题:消费者组 ,分区 ,偏移量
        //获取jedis
        val jedis = RedisUtil.getJedisClient
        //topic+":"consumerGroupId" 作为hashkey
        val offsetKey = topic + ":" + consumerGroupId

        //获取[分区,offset]  Map
        val offsetMap = jedis.hgetAll(offsetKey).asScala

        jedis.close()

        if(offsetMap!=null&&offsetMap.size>0){
            val kafkaOffsetMap = offsetMap.toList
              .map { case (partition, offset) =>
                  (new TopicPartition(topic, partition.toInt), offset.toLong)
              }
              .toMap
            kafkaOffsetMap
        } else {
            null
        }
    }


    def saveOffset(topic:String,consumerGroupId:String,offsetRanges:Array[OffsetRange]) = {
        //redis hash  主题:消费者组 --->分区 ---> 偏移量

        //topic+":"consumerGroupId" 作为hashkey
        val offsetKey = topic + ":" + consumerGroupId

        //分区内,偏移量 map
        val offsetMap = new java.util.HashMap[String, String]()

        for (offsetRange <- offsetRanges) {
            val partition = offsetRange.partition.toString
            val untilOffset = offsetRange.untilOffset.toString
            offsetMap.put(partition,untilOffset)
        }

        //获取jedis
        val jedis = RedisUtil.getJedisClient
        jedis.hmset(offsetKey,offsetMap)
        jedis.close()

    }
}
