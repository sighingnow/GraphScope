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

  val numEdges = edgeManager.getPartialEdgeNum(startLid, endLid)
  log.info("Creating JavaEdgePartition {}", this)

  def iterator : Iterator[Edge[ED]] = {
    edgeManager.iterator(startLid, endLid)
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[VD, ED2] = {
    val newData = new java.util.ArrayList[ED2](numEdges.toInt)
    val iter = iterator;
    var ind = 0;
    while (iter.hasNext){
      newData.set(ind, f(iter.next()))
      ind += 1
    }
    new GrapeEdgePartition[VD,ED2](pid, numPartitions, idManager, edgeManager.withNewEdgeData[ED2](newData, startLid, endLid))
  }


  override def toString: String = "JavaEdgePartition{" + "vertexIdManager=" + idManager + ", edgeManager=" + edgeManager + ", pid=" + pid + ", startLid=" + startLid + ", endLid=" + endLid + ", totalNumEdges=" + numEdges + '}'
}
