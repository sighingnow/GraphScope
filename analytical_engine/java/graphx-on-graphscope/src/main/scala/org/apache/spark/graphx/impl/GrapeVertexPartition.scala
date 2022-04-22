package org.apache.spark.graphx.impl

import scala.reflect.ClassTag

class GrapeVertexPartition[VD : ClassTag](grapeVertexMapPartition: GrapeVertexMapPartition, defaultValue : VD) {
  val innerVertexNum: Int = grapeVertexMapPartition.innerVertexNum
  val outerVertexNum: Int = grapeVertexMapPartition.outerVertexNum
  val totalVertexNum : Int = grapeVertexMapPartition.totalVertexNum

  val data = Array.fill(totalVertexNum)(defaultValue)

  val innerVertices : Range = 0 until innerVertexNum
  val outerVertices : Range = innerVertexNum until totalVertexNum

}
