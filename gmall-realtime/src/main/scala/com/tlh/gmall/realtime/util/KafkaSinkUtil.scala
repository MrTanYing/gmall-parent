package com.tlh.gmall.realtime.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/24 23:59
 * @Version 1.0
 */
object KafkaSinkUtil {
    private val properties: Properties = PropertiesUtil.load("config.properties")
    val broker_list = properties.getProperty("kafka.broker.list")
    var kafkaProducer: KafkaProducer[String, String] = null

    def createKafkaProducer: KafkaProducer[String, String] = {
        val properties = new Properties
        properties.put("bootstrap.servers", broker_list)
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("enable.idempotence",(true: java.lang.Boolean))

        var producer: KafkaProducer[String, String] = null
        try

            producer = new KafkaProducer[String, String](properties)
        catch {
            case e: Exception =>
                e.printStackTrace()
        }
        producer
    }

    def send(topic: String, msg: String): Unit = {
        if (kafkaProducer == null) kafkaProducer = createKafkaProducer
        kafkaProducer.send(new ProducerRecord[String, String](topic, msg))

    }

    def send(topic: String,key:String, msg: String): Unit = {
        if (kafkaProducer == null) kafkaProducer = createKafkaProducer
        kafkaProducer.send(new ProducerRecord[String, String](topic,key, msg))

    }

}
