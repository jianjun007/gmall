package com.atguigu.handler

import com.atguigu.beam.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author JianJun
 * @create 2021/11/30 18:58
 */
object DauHandler {


  /**
   * 进行批次间去重
   *
   * @param startUpLogDStream
   * @return
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {

    //方案一:会对每条数据创建连接
    /*val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
      //创建Jedis连接
      val jedis = new Jedis("hadoop102", 6379)

      //redisKey
      val redisKey: String = "DAU:" + log.logDate
      //对比数据，存在返回true,不存在返回false
      val boolean: lang.Boolean = jedis.sismember(redisKey, log.mid)

      //关闭连接
      jedis.close()
      //返回false过滤,true返回一个新的DStream
      !boolean
    })
    value*/

    //方案二:在每个分区下创建连接,以减少连接个数
    /*val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      //在分区下创建redis连接
      val jedis = new Jedis("hadoop102", 6379)
      val logs: Iterator[StartUpLog] = partition.filter(log => {
        val redisKey: String = "DAU:" + log.logDate
        val boolean: lang.Boolean = jedis.sismember(redisKey, log.mid)
        !boolean
      })
      jedis.close()
      logs
    })
    value
     */

    //方案三:在每个批次内创建一次连接,来优化连接个数
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1.获取redis连接
      val jedis = new Jedis("hadoop102", 6379)

      //2.查redis中的mid
      //获取redisKey
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))

      val mids: util.Set[String] = jedis.smembers(redisKey)

      //3.将数据广播至executor端
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //4.根据获取到的mid去重
      val midFilterRDD: RDD[StartUpLog] = rdd.filter(log => {
        !midBC.value.contains(log.mid)
      })

      //关闭连接
      jedis.close()
      midFilterRDD

    })
    value
  }

  /**
   * 批次内进行去重
   * @param filterByRedisDStream
   */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    val value: DStream[StartUpLog] = {
      //1.将数据转换为K,V -> ((mid,logDate),log)
      val midAndDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.mapPartitions(partition => {
        partition.map(log => {
          ((log.mid, log.logDate), log)
        })
      })

      //2.groupByKey将相同的Key的数据聚合到同一个分区中
      val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDateToLogDStream.groupByKey()

      //3.将数据排序,并取第一条数据
      val midAndDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      })

      //4.将数据扁平化
      midAndDateToLogListDStream.flatMap(_._2)
    }
    value
  }



  /**
   * 将去重后的数据保存至Redis,为了下一批数据去重用
   *
   * @param startUpLogDStream
   * @return 写库操作,首先考虑foreachRDD,因为不需要返回值,只需要写入库
   */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {

    startUpLogDStream.foreachRDD(rdd => {
      //在这里创建连接会有序列化问题
      rdd.foreachPartition(partition => {
        //1.创建连接
        val jedisClient = new Jedis("hadoop102", 6379)
        //2.写库
        partition.foreach(log => {
          //redisKey
          val redisKey: String = "DAU:" + log.logDate
          //将mid存入redis
          jedisClient.sadd(redisKey, log.mid)
        })
        //关闭连接
        jedisClient.close()

      })

    })

  }

}
