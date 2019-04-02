package com.atguigu.gmall1018.dw.realtime.handler

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.atguigu.gmall1018.dw.realtime.bean.StartupLog
import com.atguigu.gmall1018.dw.realtime.util.PropertiesUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauHandler {

  def calcDau(startupLogDstream: DStream[StartupLog], sc: SparkContext): DStream[StartupLog] = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    val jedisDriver = new Jedis(properties.getProperty("redis.host"), properties.getProperty("redis.port").toInt)
    val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val dauKey = "dau:" + dateString

    val filteredDstream: DStream[StartupLog] = startupLogDstream.transform { rdd: RDD[StartupLog] =>
      val dauSet: util.Set[String] = jedisDriver.smembers(dauKey)
      val dauBC: Broadcast[util.Set[String]] = sc.broadcast(dauSet)
      println("" + rdd.count())
      val filteredRDD: RDD[StartupLog] = rdd.filter(startuplog =>
        !dauBC.value.contains(startuplog.mid)
      )
      println("" + filteredRDD.count())

      filteredRDD
    }

    //redis
    filteredDstream.foreachRDD { rdd: RDD[StartupLog] =>
      rdd.foreachPartition { logItr =>
        val jedisExecutor = new Jedis(properties.getProperty("redis.host"), properties.getProperty("redis.port").toInt)
        val listbuffer = new ListBuffer[Any]()
        for (startUpLog <- logItr) {
          if (!jedisExecutor.sismember(dauKey, startUpLog.mid)) {
            jedisExecutor.sadd(dauKey, startUpLog.mid)
          }
        }

      }
    }
    filteredDstream
  }


}
