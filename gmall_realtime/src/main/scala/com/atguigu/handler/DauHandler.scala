package com.atguigu.handler

import com.atguigu.beam.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.lang

/**
 * @author JianJun
 * @create 2021/11/30 18:58
 */
object DauHandler {

  /**
   *进行批次间去重
   * @param startUpLogDStream
   * @return
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {

/*        //方案一:会对每条数据创建连接
        val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
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
    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
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
