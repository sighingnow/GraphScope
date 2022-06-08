package org.apache.spark.graphx.rdd

import org.apache.spark.SparkContext
import org.apache.spark.graphx.impl.GrapeGraphImpl
import org.apache.spark.graphx.scheduler.cluster.ExecutorInfoHelper
import org.apache.spark.graphx.{GrapeEdgeRDD, GrapeVertexRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{ParallelCollectionRDD, RDD}

import scala.reflect.ClassTag

/**
 * Provide utils to interact with GraphScope.
 */
object GraphScopeRDD extends Logging{

  def loadFragmentAsRDD[VD: ClassTag, ED: ClassTag](sc : SparkContext, objectIDs : String, fragName : String) : (GrapeVertexRDD[VD],GrapeEdgeRDD[ED]) = {

//    val objectsSplited: Array[String] = objectIDs.split(",")
//    val map: mutable.Map[String,Long] = mutable.Map[String, Long]()
//    for (str <- objectsSplited){
//      val hostId = str.split(":")
//      require(hostId.length == 2)
//      val host = hostId(0)
//      val id = hostId(1)
//      require(!map.contains(host), s"entry for host ${host} already set ${map.get(host)}")
//      map(host) = id.toLong
//      log.info(s"host ${host}: objectId : ${id}")
//    }
//    val res = map.map(tuple => (tuple._2, Array(tuple._1))).toArray
//    val distributedObjectIDs = makeRDD(sc,res)
//    distributedObjectIDs.foreachPartition(iter => log.info(s"one partition on ${InetAddress.getLocalHost.getHostName}"))
//    log.info(s"${distributedObjectIDs.collect().mkString("Array(", ", ", ")")}")

    val fragmentRDD = new FragmentRDD[VD,ED](sc, ExecutorInfoHelper.getExecutors(sc), fragName,objectIDs)
    fragmentRDD.generateRDD()
  }

  def loadFragmentAsGraph[VD: ClassTag, ED: ClassTag](sc : SparkContext, objectIDs : String, fragName : String) : GrapeGraphImpl[VD,ED] = {
    val (vertexRDD,edgeRDD) = loadFragmentAsRDD[VD,ED](sc, objectIDs, fragName);
    GrapeGraphImpl.fromExistingRDDs[VD,ED](vertexRDD,edgeRDD)
  }

  def makeRDD(sc : SparkContext, seq: Array[(Long, Array[String])]): RDD[Long] ={
    val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2.toSeq)).toMap
    new ParallelCollectionRDD[Long](sc, seq.map(_._1), math.max(seq.size, 1), indexToPrefs)
  }

//  def getExecutorHostNames(sc : SparkContext)  : Array[String] = {
////    val castedBackend = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
//    val hostNames = sc.getExecutorMemoryStatus.map(t => t._1.split(":")(0)).toArray
//    log.info(s"launched host names ${hostNames.mkString("Array(", ", ", ")")}")
////    castedBackend.getExecutorAvailableResources()
////    log.info(s"${castedBackend.getClass.getDeclaredFields.mkString("Array(", ", ", ")")}")
////    val field = castedBackend.getClass.getDeclaredField("executorHosts")
////    require(field != null)
////    field.setAccessible(true)
////
////    val executorIps = field.get(castedBackend).asInstanceOf[mutable.HashMap[String,String]]
////    log.info(s"${executorIps.values.toArray.mkString("Array(", ", ", ")")}")
////    val executorHosts = executorIps.values.toArray.map(ip => {
////      val inetAddress = InetAddress.getByName(ip)
////      inetAddress.getHostName
////    })
////    log.info(s"transformed to hostnames: ${executorHosts.mkString("Array(", ", ", ")")}")
//    hostNames
//  }

}
