package org.apache.spark.graphx


class ReusableEdge[ED] extends Edge[ED] {
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
