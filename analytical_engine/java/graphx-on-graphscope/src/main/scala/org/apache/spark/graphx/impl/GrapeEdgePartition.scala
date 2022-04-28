package org.apache.spark.graphx.impl

import org.apache.spark.graphx.traits.{EdgeManager, GraphXVertexIdManager}
import org.apache.spark.graphx.{Edge, PartitionID}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GrapeEdgePartition[VD: ClassTag, ED : ClassTag](
                                                       val pid : PartitionID, numPartitions: Int,
                                                       idManager: GraphXVertexIdManager, edgeManager: EdgeManager[VD, ED]) extends Logging{
  val totalVnum: Long = idManager.getInnerVerticesNum
  val chunkSize: Long = (totalVnum + (numPartitions - 1)) / numPartitions
  val startLid = Math.min(chunkSize * pid, totalVnum)
  val endLid = Math.min(startLid + chunkSize, totalVnum)

  val numEdges = edgeManager.getPartialEdgeNum(startLid, endLid)
  log.info("Creating JavaEdgePartition {}", this)

  def iterator : Iterator[Edge[ED]] = {
    edgeManager.iterator(startLid, endLid)
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[VD, ED2] = {
    val newData = new Array[ED2](numEdges.toInt)
    val iter = iterator;
    var ind = 0;
    while (iter.hasNext){
      newData(ind) =  f(iter.next())
      ind += 1
    }
    new GrapeEdgePartition[VD,ED2](pid, numPartitions, idManager, edgeManager.withNewEdgeData[ED2](newData, startLid, endLid))
  }


  override def toString: String = "JavaEdgePartition{" + "vertexIdManager=" + idManager + ", edgeManager=" + edgeManager + ", pid=" + pid + ", startLid=" + startLid + ", endLid=" + endLid + ", totalNumEdges=" + numEdges + '}'
}
