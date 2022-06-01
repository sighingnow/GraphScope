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

  def loadFragmentAsRDD[VD: ClassTag, ED: ClassTag](sc : SparkContext, objectID : String, fragName : String) : (GrapeVertexRDD[VD],GrapeEdgeRDD[ED]) = {
    val fragmentRDD = new FragmentRDD[VD,ED](sc, getExecutorHostNames(sc), fragName,objectID)
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
