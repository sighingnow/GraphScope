package org.apache.spark.graphx.traits

import com.alibaba.graphscope.utils.MappedBuffer

import scala.reflect.ClassTag


trait VertexDataManager[VD] {
  def getVDClz : Class[VD]
  def getVertexData(lid: Long): VD

  def setVertexData(lid: Long, vertexData: VD): Unit

  def withNewVertexData[VDATA_T2 : ClassTag](newVertexData: Array[VDATA_T2]): VertexDataManager[VDATA_T2]

  def setValues(vdatas: Array[VD]): Unit

  def writeBackVertexData(buffer : MappedBuffer) : Unit
}
