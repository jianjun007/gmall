package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.util.control.Breaks.{break, breakable}

/**
 * @author JianJun
 * @create 2021/12/6 9:08
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //4.将数据转化成样例类(EventLog文档中有)，补充时间字段，将数据转换为（k，v） k->mid  v->log
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(redord => {
      //将数据转换为样例类
      val eventLog: EventLog = JSON.parseObject(redord.value(), classOf[EventLog])
      //补充日期,小时字段
      eventLog.logDate = sdf.format(new Date(eventLog.ts)).split(" ")(0)
      eventLog.logHour = sdf.format(new Date(eventLog.ts)).split(" ")(1)

      //返回值k->mid  v->log
      (eventLog.mid, eventLog)

    })


    //5.开窗,开窗大小5min,滑动步长默认等于批次时间5s
    val windowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //6.分组聚合按照mid
    val midToLogItermDStream: DStream[(String, Iterable[EventLog])] = windowDStream.groupByKey()

    //7.筛选数据，首先用户得领优惠券，并且用户没有浏览商品行为（将符合这些行为的uid保存下来至set集合）
    val boolDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogItermDStream.mapPartitions(iter =>

      iter.map { case (mid, iter) => {
        //创建set集合用来保存uid
        val uids = new util.HashSet[String]()
        //创建set集合用来保存优惠卷所涉及商品id
        val itemId = new util.HashSet[String]()
        //创建List集合用来保存用户行为事件
        val events = new util.ArrayList[String]()


        //标志位,用来判断是否有浏览商品行为
        var bool = true
        //判断有没有浏览商品行为
        breakable {
          iter.foreach(log => {
            events.add(log.evid)
            if (log.evid.equals("clickItem")) {
              bool = false
              break()
            } else if (log.evid.equals("coupon")) {
              itemId.add(log.itemid)
              uids.add(log.uid)
            }
          })
        }
        //产生疑似预警日志
        ((bool && uids.size() >= 3), CouponAlertInfo(mid, uids, itemId, events, System.currentTimeMillis()))
      }
      })

    //8.生成预警日志(将数据保存至CouponAlertInfo样例类中，文档中有)，条件：符合第七步要求，并且uid个数>=3（主要为“过滤”出这些数据），实质：补全CouponAlertInfo样例类
    val alterDstream: DStream[CouponAlertInfo] = boolDStream.filter(_._1).map(_._2)
    alterDstream.print()

    //9.将预警数据写入ES
    alterDstream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val indexName = GmallConstants.ES_ALERT_INDEXNAME + sdf.format(new Date((System.currentTimeMillis()))).split(" ")(0)
        val list: List[(String, CouponAlertInfo)] = iter.toList.map(alert => {
          (alert.mid + alert.ts / 1000 / 60, alert)
        })
        MyEsUtil.insertBulk(indexName, list)

      })
    })


    //10.开启任务并阻塞
    ssc.start()
    ssc.awaitTermination()

  }

}
