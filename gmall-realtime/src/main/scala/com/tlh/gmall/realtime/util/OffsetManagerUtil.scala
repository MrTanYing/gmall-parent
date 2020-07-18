package com.tlh.gmall.realtime.util

import org.apache.kafka.common.TopicPartition

import  scala.collection.JavaConverters._

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/18 15:22
 * @Version 1.0
 */
object OffsetManagerUtil {
    def getOffset(topic:String,consumerGroupId:String) = {

        //获取jedis
        val jedis = RedisUtil.getJedisClient
        //topic+":"consumerGroupId" 最为hashkey
        val offsetKey = topic + ":" + consumerGroupId

        //分区,offset Map
        val offsetMap = jedis.hgetAll(offsetKey).asScala

        jedis.close()

        val offsetList: List[(String, String)]  = offsetMap.toList

        val kafkaOffsetList: List[(TopicPartition, Long)] = offsetList.map {
            case ( partition, offset) =>
            (new TopicPartition(topic, partition.toInt), offset.toLong)
        }
        kafkaOffsetList.toMap
    }
}
