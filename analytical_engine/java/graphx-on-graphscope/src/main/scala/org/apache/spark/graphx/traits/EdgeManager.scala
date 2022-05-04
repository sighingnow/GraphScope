package org.apache.spark.graphx.traits

import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graphx.GSEdgeTriplet
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.{Edge, EdgeTriplet, TripletFields, VertexId}

import scala.reflect.ClassTag

trait EdgeManager[VD,ED] {
  def iterator(startLid: Long, endLid: Long): Iterator[Edge[ED]]

  def aggregateVertexAttr(startLid : Long, endLid : Long, vdArray : Array[VD]) : Unit
  /**
   *For graphx ops, we need to pass vd array for actual vdata
   * @return
   */
  def tripletIterator(startLid: Long, endLid: Long, tripletFields: TripletFields = TripletFields.All): Iterator[EdgeTriplet[VD,ED]]
  /**
   * Get the num edges between [startLid, endLid)
   * @param startLid
   * @param endLid
   * @return
   */
  def getPartialEdgeNum(startLid: Long, endLid: Long): Long

  def getDegreeArray(startLid : Long, endLid : Long) : Array[Int];

  def getTotalEdgeNum: Long

  def iterateOnEdgesParallel[MSG](tid: Int, srcLid: Long, context: GSEdgeTriplet[VD, ED],
                                  msgSender: EdgeTriplet[VD, ED] => Iterator[(VertexId, MSG)], outMessageCache: MessageStore[MSG]): Unit

  def withNewEdgeData[ED2 : ClassTag](newEdgeData: PrimitiveArray[ED2], startLid: Long, endLid: Long): EdgeManager[VD, ED2]

  /**
   * Reverse src,dst pairs. return a new edgeManager.
   * This reverse will not write back to c++ memory.
   * @param startLid start vid
   * @param endLid end vid
   */
  def reverseEdges() : EdgeManager[VD,ED]

  /**
   * Return a new edge manager, will only partial of the original data.
   * Vertex data manager should be updated before calling this
   * @param epred
   * @param vpred
   * @return
   */
  def filter(epred: EdgeTriplet[VD, ED] => Boolean,
             vpred: (VertexId, VD) => Boolean, startLid : Long, endLid : Long) : EdgeManager[VD,ED]

  def innerJoin[ED2 : ClassTag, ED3 : ClassTag](edgeManager: EdgeManager[_,ED2], startLid : Long, endLid : Long)(f: (VertexId, VertexId, ED, ED2) => ED3): EdgeManager[VD,ED3]
}
