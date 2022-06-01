package org.apache.spark.graphx.rdd

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeRDD, GrapeEdgeRDD, GrapeVertexRDD, VertexRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{ParallelCollectionRDD, RDD}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, ExecutorData, ExecutorInfo, StandaloneSchedulerBackend}

import java.net.InetAddress
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

/**
 * Provide utils to interact with GraphScope.
 */
object GraphScopeRDD extends Logging{

  def loadFragmentAsRDD[VD: ClassTag, ED: ClassTag](sc : SparkContext, objectIDs : String, fragName : String) : (GrapeVertexRDD[VD],GrapeEdgeRDD[ED]) = {

    val tmpRdd = sc.parallelize(0 to 100, 100).coalesce(2)
    val collectedHostNames = tmpRdd.mapPartitions( iter => Iterator(InetAddress.getLocalHost.getHostName)).collect().distinct
    log.info(s"Collected host names : ${collectedHostNames.mkString("Array(", ", ", ")")}")
    val objectsSplited: Array[String] = objectIDs.split(",")
    val map: mutable.Map[String,Long] = mutable.Map[String, Long]()
    for (str <- objectsSplited){
      val hostId = str.split(":")
      require(hostId.length == 2)
      val host = hostId(0)
      val id = hostId(1)
      require(!map.contains(host), s"entry for host ${host} already set ${map.get(host)}")
      map(host) = id.toLong
      log.info(s"host ${host}: objectId : ${id}")
    }
    val res = map.map(tuple => (tuple._2, Array(tuple._1))).toArray
    val distributedObjectIDs = makeRDD(sc,res)
    distributedObjectIDs.foreachPartition(iter => log.info(s"one partition on ${InetAddress.getLocalHost.getHostName}"))
    log.info(s"${distributedObjectIDs.collect().mkString("Array(", ", ", ")")}")

    val fragmentRDD = new FragmentRDD[VD,ED](sc, getExecutorHostNames(sc),  getExecutorIdss(sc),fragName,objectIDs)
    fragmentRDD.generateRDD()
  }

  def makeRDD(sc : SparkContext, seq: Array[(Long, Array[String])]): RDD[Long] ={
    val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2.toSeq)).toMap
    new ParallelCollectionRDD[Long](sc, seq.map(_._1), math.max(seq.size, 1), indexToPrefs)
  }


  def getExecutorHostNames(sc : SparkContext)  : Array[String] = {
    val castedBackend = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
    log.info(s"${castedBackend.getClass.getDeclaredFields.mkString("Array(", ", ", ")")}")
    val field = castedBackend.getClass.getDeclaredField("executorHosts")
    require(field != null)
    field.setAccessible(true)

    val executorIps = field.get(castedBackend).asInstanceOf[mutable.HashMap[String,String]]
    log.info(s"${executorIps.values.toArray.mkString("Array(", ", ", ")")}")
    val executorHosts = executorIps.values.toArray.map(ip => {
      val inetAddress = InetAddress.getByName(ip)
      inetAddress.getHostName
    })
    log.info(s"transformed to hostnames: ${executorHosts.mkString("Array(", ", ", ")")}")
    executorHosts
  }

  def getExecutorIdss(sc : SparkContext)  : Array[String] = {
    val castedBackend = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
    log.info(s"${castedBackend.getClass.getDeclaredFields.mkString("Array(", ", ", ")")}")
    val field = castedBackend.getClass.getDeclaredField("executorHosts")
    require(field != null)
    field.setAccessible(true)

    val executorIps = field.get(castedBackend).asInstanceOf[mutable.HashMap[String,String]]
    log.info(s"${executorIps.keySet.toArray.mkString("Array(", ", ", ")")}")
    executorIps.keySet.toArray
  }
}
