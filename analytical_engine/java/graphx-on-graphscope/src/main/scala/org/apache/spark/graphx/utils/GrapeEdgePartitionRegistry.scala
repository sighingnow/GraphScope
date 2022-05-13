package org.apache.spark.graphx.utils

import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.impl.partition.{EdgeShuffleReceived, GrapeEdgePartition, GrapeEdgePartitionBuilder}
import org.apache.spark.internal.Logging

import java.lang.reflect.Field
import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

class GrapeEdgePartitionRegistry[VD: ClassTag, ED: ClassTag] extends Logging{

  private val partitionCnt : AtomicInteger = new AtomicInteger(0)
  val edClass: Class[ED] = GrapeUtils.getRuntimeClass[ED].asInstanceOf[Class[ED]]

  var edgePartitionBuilder = new GrapeEdgePartitionBuilder[VD,ED](ExecutorUtils.getVineyarClient)
  log.info(s"[GrapeEdgePartitionRegistry]: Got edge PartitionBuilder ${edgePartitionBuilder}")


  def addEdgesToBuilder(pid : Int, shuffles : EdgeShuffleReceived[ED]) : Unit = {
      synchronized {
        edgePartitionBuilder.addEdges(shuffles)
        log.info(s"[GrapeEdgePartitionRegistry]: partition ${pid} finish adding edges ${shuffles}")
      }
  }

  def buildLocalVertexMap(pid : Int) : Unit = {
    synchronized{
      if (!edgePartitionBuilder.isLocalBuilt()){
        val localVMID = edgePartitionBuilder.buildLocalVertexMap()
        ExecutorUtils.setLocalVMID(localVMID)
        log.info(s"[GrapeEdgePartitionRegistry] Partition ${pid} built edge Partition")
      }
    }
  }

  def buildCSR(pid : Int) : Unit = {
    synchronized{
      if (!edgePartitionBuilder.isCSRBuilt()){
        val csrID = edgePartitionBuilder.buildCSR()
        log.info(s"[GrapeEdgePartitionRegistry] Partition ${pid} built CSR ${csrID}")
        ExecutorUtils.setCSRID(csrID)
      }
    }
  }

  /** We can not use pid as index, since on one executor the partiton num may be not necessarily consecutive */
  def getEdgePartition(pid : Int): GrapeEdgePartition[VD,ED] ={
    synchronized{
      val res = edgePartitionBuilder.getEdgePartition(pid)
      log.info(s"[GrapeEdgePartitionRegistry] Part ${pid} got edgePartition ${res}")
    }
    null
  }
}

object GrapeEdgePartitionRegistry extends Logging{
  System.loadLibrary("grape-jni")
  log.info("[GrapeEdgePartitionRegistry:] load jni lib success")
  private var registry = null.asInstanceOf[GrapeEdgePartitionRegistry[_,_]]
  def getOrCreate[VD: ClassTag,ED : ClassTag] : GrapeEdgePartitionRegistry[VD,ED] = {
    if (registry == null){
      synchronized{
        if (registry == null){
          registry = new GrapeEdgePartitionRegistry[VD,ED]
        }
      }
    }
    registry.asInstanceOf[GrapeEdgePartitionRegistry[VD,ED]]
  }

  /**
   * Clear for the next time usages.
   */
  def clear(): Unit ={
    registry = null
  }
}
object ClassScope {
  private var LIBRARIES: Field = null
  try LIBRARIES = classOf[ClassLoader].getDeclaredField("loadedLibraryNames")
  catch {
    case e: NoSuchFieldException =>
      e.printStackTrace()
  }
  LIBRARIES.setAccessible(true)

  @throws[IllegalAccessException]
  def getLoadedLibraries(loader: ClassLoader): Array[String] = {
    val libraries: java.util.HashSet[String] = LIBRARIES.get(loader).asInstanceOf[java.util.HashSet[String]]
    libraries.toArray(new Array[String](libraries.size()))
  }
}
