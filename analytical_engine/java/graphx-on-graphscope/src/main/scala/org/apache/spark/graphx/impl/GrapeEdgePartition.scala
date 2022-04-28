package org.apache.spark.graphx.impl

import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.traits.{EdgeManager, GraphXVertexIdManager}
import org.apache.spark.graphx.{Edge, EdgeTriplet, PartitionID, TripletFields, VertexId}
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

  def tripletIterator(
                       tripletFields: TripletFields)
  : Iterator[EdgeTriplet[VD, ED]] = {
    edgeManager.tripletIterator(startLid, endLid, tripletFields)
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[VD, ED2] = {
    val newData : PrimitiveArray[ED2] = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED2].asInstanceOf[Class[ED2]], numEdges.toInt)
    val iter = iterator;
    var ind = 0;
    while (iter.hasNext){
      newData.set(ind, f(iter.next()))
      ind += 1
    }
    withData(newData)
  }

  def map[ED2: ClassTag](iter: Iterator[ED2]): GrapeEdgePartition[VD, ED2] = {
    // Faster than iter.toArray, because the expected size is known.
    val newData : PrimitiveArray[ED2] = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED2].asInstanceOf[Class[ED2]], numEdges.toInt)
    var i = 0
    while (iter.hasNext) {
      newData.set(i,iter.next())
      i += 1
    }
    assert(newData.size() == i)
    this.withData(newData)
  }

  def withData[ED2: ClassTag](newData: PrimitiveArray[ED2]): GrapeEdgePartition[VD,ED2] ={
    new GrapeEdgePartition[VD,ED2](pid, numPartitions, idManager, edgeManager.withNewEdgeData[ED2](newData, startLid, endLid))
  }

  /**
   * Reverse all the edges in this partition.
   *
   * @return a new edge partition with all edges reversed.
   */
  def reverse: GrapeEdgePartition[VD, ED] = {
    //A copy of edgemanager is created.
    new GrapeEdgePartition[VD,ED](pid, numPartitions, idManager, edgeManager.reverseEdges())
  }

  /**
   * Construct a new edge partition containing only the edges matching `epred` and where both
   * vertices match `vpred`.
   */
  def filter(
              epred: EdgeTriplet[VD, ED] => Boolean,
              vpred: (VertexId, VD) => Boolean): GrapeEdgePartition[VD, ED] = {
    new GrapeEdgePartition[VD,ED](pid, numPartitions, idManager, edgeManager.filter(epred, vpred, startLid, endLid))
  }


  override def toString: String = "JavaEdgePartition{" + "vertexIdManager=" + idManager + ", edgeManager=" + edgeManager + ", pid=" + pid + ", startLid=" + startLid + ", endLid=" + endLid + ", totalNumEdges=" + numEdges + '}'
}
