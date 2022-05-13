package org.apache.spark.graphx.utils

import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.impl.partition.{GrapeVertexPartition, GrapeVertexPartitionBuilder}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GrapeVertexPartitionRegistry[VD : ClassTag] extends Logging{
  val vdClz = GrapeUtils.getRuntimeClass[VD].asInstanceOf[Class[VD]]
  val vertexPartitionBuilder = new GrapeVertexPartitionBuilder[VD]

  def checkPrerequisite(pid : Int) : Unit = {
    synchronized{
      require(ExecutorUtils.isPartitionRegistered(pid))
      require(ExecutorUtils.checkBeforeVertexPartition(pid))
    }
  }

  def init(pid : Int, initVal : VD) : Unit = {
    synchronized{
      if (!vertexPartitionBuilder.isInitialized){
        val fragVertices = ExecutorUtils.getGlobalVM.getVertexSize
        log.info(s"Partition ${pid} doing initialization with default value ${initVal}, frag vertices ${fragVertices}")
        vertexPartitionBuilder.init(fragVertices, initVal)
      }
    }
  }

  def build(pid : Int) : Unit = {
    synchronized {
      if (!vertexPartitionBuilder.isBuilt){
        vertexPartitionBuilder.build(pid)
      }
//      vertexPartitionBuilder.getResult
    }
  }

  def getVertexPartition(pid : Int) : GrapeVertexPartition[VD] = {
    synchronized{
      val res = vertexPartitionBuilder.getVertexPartition(pid)
      log.info(s"[GrapeVertexPartitionRegistry] Part ${pid} got edgePartition ${res}")
      res
    }
  }
}
object GrapeVertexPartitionRegistry{
  private var registry = null.asInstanceOf[GrapeVertexPartitionRegistry[_]]
  def getOrCreate[VD: ClassTag] : GrapeVertexPartitionRegistry[VD] = {
    if (registry == null){
      synchronized{
        if (registry == null){
          registry = new GrapeVertexPartitionRegistry[VD]
        }
      }
    }
    registry.asInstanceOf[GrapeVertexPartitionRegistry[VD]]
  }

  /**
   * Clear for the next time usages.
   */
  def clear(): Unit ={
    registry = null
  }
}
