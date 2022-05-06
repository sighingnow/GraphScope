package org.apache.spark.graphx.impl

import com.alibaba.graphscope.graphx.GrapeEdgePartition
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.impl.graph.EdgeManagerImpl
import org.apache.spark.graphx.traits.{EdgeManager, GraphXVertexIdManager}
import org.apache.spark.graphx.{Edge, EdgeTriplet, PartitionID, TripletFields, VertexDataManagerCreator, VertexId}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

/**
 *
 * GrapeEdgePartition is a wrapper of c++ edge Partition, descipted by (startLid, endLid).
 * All grape edge Partitions on same executor share the same index.
 */
class GrapeEdgePartitionWrapper[VD: ClassTag, ED : ClassTag](
                                                       val pid : PartitionID,
                                                       val startLid : Long,
                                                       val endLid : Long,
                                                       val grapePartition : GrapeEdgePartition[Long,Long,ED]) extends Logging{
//  val totalVnum: Long = idManager.getInnerVerticesNum
//  val chunkSize: Long = (totalVnum + (numPartitions - 1)) / numPartitions
//  val startLid = Math.min(chunkSize * pid, totalVnum)
//  val endLid = Math.min(startLid + chunkSize, totalVnum)

//  val numEdges = edgeManager.getPartialEdgeNum(startLid, endLid)
  log.info("Creating JavaEdgePartition {}", this)

  def createNewVDManager[VD2 : ClassTag] : Unit = {
//    VertexDataManagerCreator.create[VD,VD2,ED](pid, edgeManager.asInstanceOf[EdgeManagerImpl[VD,ED]].vertexDataManager)
  }

  def getDegreeArray(startLid : Long, endLid : Long) : Array[Int] = {
//    edgeManager.getDegreeArray(startLid, endLid)
    null
  }

  def iterator : Iterator[Edge[ED]] = {
//    edgeManager.iterator(startLid, endLid)
    null
  }

  def aggregateVertexAttr(startLid : Long, endLid : Long, vdArray : Array[VD]) : GrapeEdgePartitionWrapper[VD,ED] = {
//    edgeManager.aggregateVertexAttr(startLid, endLid, vdArray)
//    this
    null
  }

  def tripletIterator(tripletFields: TripletFields = TripletFields.All)
  : Iterator[EdgeTriplet[VD, ED]] = {
    //!can not use edge manager here!!!!!!!
    //edgeManager.tripletIterator(startLid, endLid, tripletFields)
//    edgeManager.tripletIterator(startLid, endLid,tripletFields)
    null
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartitionWrapper[VD, ED2] = {
//    val newData : PrimitiveArray[ED2] = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED2].asInstanceOf[Class[ED2]], numEdges.toInt)
//    val iter = iterator;
//    var ind = 0;
//    while (iter.hasNext){
//      newData.set(ind, f(iter.next()))
//      ind += 1
//    }
//    withData(newData)
    null
  }

  def map[ED2: ClassTag](iter: Iterator[ED2]): GrapeEdgePartitionWrapper[VD, ED2] = {
    // Faster than iter.toArray, because the expected size is known.
//    val newData : PrimitiveArray[ED2] = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED2].asInstanceOf[Class[ED2]], numEdges.toInt)
//    var i = 0
//    while (iter.hasNext) {
//      newData.set(i,iter.next())
//      i += 1
//    }
//    assert(newData.size() == i)
//    this.withData(newData)
    null
  }

  def withData[ED2: ClassTag](newData: PrimitiveArray[ED2]): GrapeEdgePartitionWrapper[VD,ED2] ={
//    new GrapeEdgePartition[VD,ED2](pid, numPartitions, idManager, edgeManager.withNewEdgeData[ED2](newData, startLid, endLid))
    null
  }

  def innerJoin[ED2: ClassTag, ED3: ClassTag]
  (other: GrapeEdgePartitionWrapper[_, ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgePartitionWrapper[VD, ED3] = {
//    new GrapeEdgePartition[VD,ED3](pid, numPartitions, idManager, this.edgeManager.innerJoin[ED2,ED3](other.edgeManager,startLid, endLid)(f))
    null
  }

  /**
   * Reverse all the edges in this partition.
   *
   * @return a new edge partition with all edges reversed.
   */
  def reverse: GrapeEdgePartitionWrapper[VD, ED] = {
    //A copy of edgemanager is created.
    //
    null
  }

  /**
   * Construct a new edge partition containing only the edges matching `epred` and where both
   * vertices match `vpred`.
   */
  def filter(
              epred: EdgeTriplet[VD, ED] => Boolean,
              vpred: (VertexId, VD) => Boolean): GrapeEdgePartitionWrapper[VD, ED] = {
    null
  }

  /**
   * Merge all the edges with the same src and dest id into a single
   * edge using the `merge` function
   *
   * @param merge a commutative associative merge operation
   * @return a new edge partition without duplicate edges
   */
  def groupEdges(merge: (ED, ED) => ED): GrapeEdgePartitionWrapper[VD, ED] = {
    //suppose dst oids are sort
    this
  }

  override def toString: String = "GrapeEdgePartitionWrapper@(pid" + pid + ", from" + startLid + " to " + endLid + ", edgePartition: " + grapePartition + ")";
}
