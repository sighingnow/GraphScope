package org.apache.spark.graphx.rdd

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeRDD, GrapeEdgeRDD, GrapeVertexRDD, VertexRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, ExecutorData, ExecutorInfo, StandaloneSchedulerBackend}

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
    log.info(s"backend: ${sc.schedulerBackend}")
    val castedBackend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
    val executorDataMapField = castedBackend.getClass.getDeclaredField("executorDataMap")
    executorDataMapField.setAccessible(true)
    require(executorDataMapField != null)
    val executorDataMap = executorDataMapField.get(castedBackend)
    require(executorDataMap != null)
    //ExecutorData in private in package cluster
    val readExecutorDataMap = executorDataMap.asInstanceOf[HashMap[String, ExecutorInfo]]
    val array = readExecutorDataMap.values.map(info => info.executorHost).toArray
    log.info(s"collected executor host array: ${array.mkString("Array(", ", ", ")")}")
    array
  }
}
