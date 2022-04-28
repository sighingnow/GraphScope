package org.apache.spark.graphx

trait ReusableEdge[ED] extends Edge[ED]{
  def setSrcId(vertexId: VertexId)
  def setDstId(vertexId: VertexId)
  def setAttr(ed : ED)
}

class ReusableEdgeImpl[ED] extends ReusableEdge[ED] {
  override def setSrcId(vertexId: VertexId) = {
    this.srcId = vertexId
  }
  override def setDstId(vertexId: VertexId) = {
    this.dstId = vertexId
  }

  override def setAttr(ed : ED) = {
    this.attr = ed
  }
}

class ReversedReusableEdge[ED] extends ReusableEdge[ED]{
  override def setSrcId(vertexId: VertexId) = {
    this.dstId = vertexId
  }
  override def setDstId(vertexId: VertexId) = {
    this.srcId = vertexId
  }

  override def setAttr(ed : ED) = {
    this.attr = ed
  }
}
