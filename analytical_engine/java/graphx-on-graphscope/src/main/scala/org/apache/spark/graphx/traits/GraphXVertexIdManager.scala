package org.apache.spark.graphx.traits

import com.alibaba.graphscope.graph.VertexIdManager

trait GraphXVertexIdManager extends VertexIdManager[Long,Long]{

  def getInnerVerticesNum: Long

  def getVerticesNum: Long
}
