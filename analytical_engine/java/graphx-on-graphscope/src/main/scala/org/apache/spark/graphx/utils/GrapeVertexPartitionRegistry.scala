package org.apache.spark.graphx.utils

import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.impl.partition.{GrapeVertexPartition, GrapeVertexPartitionBuilder}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GrapeVertexPartitionRegistry extends Logging{
//  val vdClz = GrapeUtils.getRuntimeClass[VD].asInstanceOf[Class[VD]]
  var vertexPartitionBuilder = null.asInstanceOf[GrapeVertexPartitionBuilder[_]]

  def clear(pid : Int) : Unit = {
    synchronized{
      if (vertexPartitionBuilder != null){
        vertexPartitionBuilder = null
      }
    }
  }

  def checkPrerequisite(pid : Int) : Unit = {
    synchronized{
      require(ExecutorUtils.isPartitionRegistered(pid))
      require(ExecutorUtils.checkBeforeVertexPartition(pid))
    }
  }

  def init[VD: ClassTag](pid : Int, initVal : VD) : Unit = {
    synchronized{
      if (vertexPartitionBuilder == null){
        vertexPartitionBuilder = new GrapeVertexPartitionBuilder[VD]
        val fragVertices = ExecutorUtils.getGlobalVM.getVertexSize
        log.info(s"Partition ${pid} doing initialization with default value ${initVal}, frag vertices ${fragVertices}")
        vertexPartitionBuilder.asInstanceOf[GrapeVertexPartitionBuilder[VD]].init(fragVertices, initVal)
      }
    }
  }
  /** Create a new vertexPartition with original partition and transformation function */
  def init[VD : ClassTag, VD2 : ClassTag](pid : Int, vertexPartition : GrapeVertexPartition[VD], map : (VertexId, VD) => VD2) : Unit = {
    synchronized{
      if (vertexPartitionBuilder == null){
        vertexPartitionBuilder = new GrapeVertexPartitionBuilder[VD2]
        log.info(s"Partition ${pid} create new Vertex Data from original Vd type ${GrapeUtils.getRuntimeClass[VD].toString} to ${GrapeUtils.getRuntimeClass[VD2].toString}")
        vertexPartitionBuilder.asInstanceOf[GrapeVertexPartitionBuilder[VD2]].init(vertexPartition, map)
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

  def getVertexPartition[VD : ClassTag](pid : Int) : GrapeVertexPartition[VD] = {
    synchronized{
      val res = vertexPartitionBuilder.asInstanceOf[GrapeVertexPartitionBuilder[VD]].getVertexPartition(pid)
      log.info(s"[GrapeVertexPartitionRegistry] Part ${pid} got edgePartition ${res}")
      res
    }
  }
}
object GrapeVertexPartitionRegistry{
  private var registry = null.asInstanceOf[GrapeVertexPartitionRegistry]
  def getOrCreate : GrapeVertexPartitionRegistry = {
    if (registry == null){
      synchronized{
        if (registry == null){
          registry = new GrapeVertexPartitionRegistry
        }
      }
    }
    registry
  }

  /**
   * Clear for the next time usages.
   */
  def clear(): Unit ={
    registry = null
  }
}
