package org.apache.spark.graphx.impl

import com.alibaba.graphscope.graph.GraphXVertexIdManager
import com.alibaba.graphscope.graph.GraphxEdgeManager
import org.apache.spark.graphx.{Edge, PartitionID}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GrapeEdgePartition[VD: ClassTag, ED : ClassTag](
     val pid : PartitionID, numPartitions: Int,
     idManager: GraphXVertexIdManager, edgeManager: GraphxEdgeManager[VD, ED, _]) extends Logging{
  val totalVnum: Long = idManager.innerVerticesNum
  val chunkSize: Long = (totalVnum + (numPartitions - 1)) / numPartitions
  val startLid = Math.min(chunkSize * pid, totalVnum)
  val endLid = Math.min(startLid + chunkSize, totalVnum)
  log.info("Creating JavaEdgePartition {}", this)

  def iterator : Iterator[Edge[ED]] = {
    edgeManager.iterator(startLid, endLid)
  }

  override def toString: String = "JavaEdgePartition{" + "vertexIdManager=" + idManager + ", edgeManager=" + edgeManager + ", pid=" + pid + ", startLid=" + startLid + ", endLid=" + endLid + '}'
}
