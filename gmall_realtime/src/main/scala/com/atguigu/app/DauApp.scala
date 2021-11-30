package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.beam.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author JianJun
 * @create 2021/11/30 17:22
 */
object DauApp {

  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    //TODO 2 创建SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //TODO 3 连接kafka
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //TODO 4 将json转换为样例类,并且补全logDate&logHour两个时间字段
    //在Driver端定义,需要到executor使用,可能会产生序列化问题,所以使用SimpleDateFormat(实现了序列化接口)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将JSON转换为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        //ts->yyyy-MM-dd HH
        val times: String = sdf.format(new Date(startUpLog.ts))
        //补全logDate
        startUpLog.logDate = times.split(" ")(0)
        //补全logHour
        startUpLog.logHour = times.split(" ")(1)
        startUpLog

      })
    })
    //优化:缓存多次使用的流
    startUpLogDStream.cache()

    //TODO 5 进行批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream)
    //优化:缓存多次使用的流
    filterByRedisDStream.cache()

    //--------------------------------测试_start---------------------------
    startUpLogDStream.count().print()

    filterByRedisDStream.count().print()

    //--------------------------------测试_end-----------------------------

    //TODO 6  进行批次内去重

    //TODO 7  将去重后的数据保存至Redis,为了下一批数据去重用
    DauHandler.saveToRedis(filterByRedisDStream)

    //TODO 8 将去重后的明细数据保存到Hbase





    /*    //打印测试
        val value: DStream[String] = kafkaDStream.mapPartitions(partition => {
          partition.map(record => {
            record.value()
          })
        })

        value.print()*/


    ssc.start()
    ssc.awaitTermination()

  }

}
