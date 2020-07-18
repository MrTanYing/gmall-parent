package com.tlh.gmall.realtime.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/17 22:10
 * @Version 1.0
 */
object PropertiesUtil {

    def main(args: Array[String]): Unit = {
        val properties: Properties = PropertiesUtil.load("config.properties")

        println(properties.getProperty("kafka.broker.list"))
    }

    def load(propertieName:String): Properties ={
        val prop=new Properties();
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
        prop
    }


}
