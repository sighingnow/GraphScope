package org.apache.spark.graphx.impl.partition.data

import com.alibaba.graphscope.graphx.VertexData

import scala.reflect.ClassTag

class GrapeVertexDataStore[VD : ClassTag](val vertexData : VertexData[Long,VD]) extends VertexDataStore [VD]{

  override def size: Long = vertexData.verticesNum()

  override def getData(lid : Long) : VD = vertexData.getData(lid)

  override def vineyardID: Long = vertexData.id()
}
