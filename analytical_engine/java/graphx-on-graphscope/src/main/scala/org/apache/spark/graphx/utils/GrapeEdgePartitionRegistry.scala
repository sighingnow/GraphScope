package org.apache.spark.graphx.utils

import com.alibaba.graphscope.graphx.GrapeEdgePartition
import com.alibaba.graphscope.utils.ReflectUtils
import org.apache.spark.graphx.impl.{GrapeEdgePartitionWrapper, GrapeUtils}
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

class GrapeEdgePartitionRegistry[VD: ClassTag, ED: ClassTag] extends Logging{
  private var registeredPath : String = null.asInstanceOf[String]
  private val partitionNum : AtomicInteger = new AtomicInteger(0)
  private var grapeEdgePartition : GrapeEdgePartition[Long,Long,ED]  = null.asInstanceOf[GrapeEdgePartition[Long,Long,ED]]
   def registerPath(pid : Int, pathStr : String) : Unit = {
     partitionNum.addAndGet(1)
     if (registeredPath == null){
       synchronized{
          if (registeredPath == null){
            log.info(s"Partition [${pid}] registered ${pathStr}")
            registeredPath = pathStr
            return
         }
       }
     }
     log.info(s"Partition [${pid}] skip registering since already registered ${registeredPath}, part num: ${partitionNum}");
  }

  def constructEdgePartition(pid : Int, size : Long) : Unit = {
    if (grapeEdgePartition == null){
      synchronized{
        if (grapeEdgePartition == null){
          log.info(s"Partition [${pid}] try to construct grape partition")
          grapeEdgePartition = ReflectUtils.invokeEdgePartitionCreation(
            classOf[java.lang.Long].asInstanceOf[Class[_ <: Long]],
            classOf[java.lang.Long].asInstanceOf[Class[_ <: Long]],
            GrapeUtils.getRuntimeClass[ED].asInstanceOf[Class[_ <: ED]], registeredPath, size)
          return
        }
      }
    }
    log.info(s"Partition [${pid}] skip construct grape partition")
  }

  def getEdgePartitionWrapper(pid : Int): GrapeEdgePartitionWrapper[VD,ED] ={
    val chunkSize = (grapeEdgePartition.getVerticesNum() + partitionNum.get() - 1) / partitionNum.get()
    val startLid = chunkSize * pid
    val endLid = Math.min(startLid + chunkSize, grapeEdgePartition.getVerticesNum)
    new GrapeEdgePartitionWrapper[VD,ED](pid, startLid, endLid, grapeEdgePartition)
  }
}

object GrapeEdgePartitionRegistry extends Logging{
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