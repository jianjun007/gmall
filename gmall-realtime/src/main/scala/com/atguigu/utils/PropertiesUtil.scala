package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author JianJun
 * @create 2021/11/30 14:09
 */
object PropertiesUtil {
  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

 //测试代码
 /* def main(args: Array[String]): Unit = {
    val properties: Properties = load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }
*/
}
