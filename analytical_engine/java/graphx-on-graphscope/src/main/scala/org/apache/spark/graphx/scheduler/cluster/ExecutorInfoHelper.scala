package org.apache.spark.graphx.scheduler.cluster

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, StandaloneSchedulerBackend}
import org.apache.spark.scheduler.cluster.ExecutorInfo

import java.net.InetAddress
import scala.collection.mutable

object ExecutorInfoHelper extends Logging{

  def getExecutors(sc : SparkContext) : mutable.HashMap[String,String] = {
    val castedBackend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
    val allFields = castedBackend.getClass.getSuperclass.getDeclaredFields
    log.info(s"${allFields.mkString(",")}")
    val field = castedBackend.getClass.getSuperclass.getDeclaredField("org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap")
    require(field != null)
    field.setAccessible(true)
    val executorDataMap = field.get(castedBackend).asInstanceOf[mutable.HashMap[String,ExecutorInfo]]
    require(executorDataMap != null)

    val res = new mutable.HashMap[String,String]()
    for (tuple <- executorDataMap){
      val executorId = tuple._1
      val host = tuple._2.executorHost
      log.info(s"executor id ${executorId}, host ${host}")
      //here we got ip, cast to hostname
      val hostName = InetAddress.getByName(host).getHostName
      res.+=((executorId,hostName))
    }
    res
  }

  def getExecutorsHost2Id(sc : SparkContext) : mutable.HashMap[String,String] = {
    val castedBackend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
    val allFields = castedBackend.getClass.getSuperclass.getDeclaredFields
//    log.info(s"${allFields.mkString(",")}")
    val field = castedBackend.getClass.getSuperclass.getDeclaredField("org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap")
    require(field != null)
    field.setAccessible(true)
    val executorDataMap = field.get(castedBackend).asInstanceOf[mutable.HashMap[String,ExecutorInfo]]
    require(executorDataMap != null)

    val res = new mutable.HashMap[String,String]()
    for (tuple <- executorDataMap){
      val executorId = tuple._1
      val host = tuple._2.executorHost
      log.info(s"executor id ${executorId}, host ${host}")
      //here we got ip, cast to hostname
      val hostName = InetAddress.getByName(host).getHostName
      if (res.contains(hostName)){
        throw new IllegalStateException(s"host ${hostName} already contains executor ${res.get(hostName)}")
      }
      res.+=((hostName,executorId))
    }
    res
  }
}
