package org.apache.spark.graphx.rdd

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeRDD, GrapeEdgeRDD, GrapeVertexRDD, VertexRDD}
import org.apache.spark.internal.Logging
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
    log.info(s"Collected host names : ${collectedHostNames}")
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
    val distributedObjectIDs = sc.makeRDD(res).repartition(2)
    log.info(s"${distributedObjectIDs.collect().map(t => (t._1, t._2(0))).mkString("Array(", ", ", ")")}")
    distributedObjectIDs.foreachPartition(iter => {
      val tuple = iter.next()
      val host = tuple._2(0)
      log.info(s"Partition on dst host ${host} get cur host ${InetAddress.getLocalHost.getHostName}")
    })

    val fragmentRDD = new FragmentRDD[VD,ED](sc, getExecutorHostNames(sc), fragName,objectIDs)
    fragmentRDD.generateRDD()
  }


  def getExecutorHostNames(sc : SparkContext)  : Array[String] = {
//    val status = sc.getExecutorMemoryStatus
//    log.info(s"Got executor memory status ${status}, size ${status.size}")
//    val hostNames = status.iterator.map(item => item._1).toArray
//    log.info(s"Got collected hostNames ${hostNames.mkString("Array(", ", ", ")")}")
//    hostNames

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
}
