package org.apache.spark.graphx

import scala.reflect.ClassTag

class ReusableEdge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] extends Edge[ED] {
  def setSrcId(vertexId: VertexId) = {
    this.srcId = vertexId
  }
  def setDstId(vertexId: VertexId) = {
    this.dstId = vertexId
  }

  def setAttr(ed : ED) = {
    this.attr = ed
  }
}
