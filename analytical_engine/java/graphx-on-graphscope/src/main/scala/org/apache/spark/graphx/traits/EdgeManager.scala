package org.apache.spark.graphx.traits

import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graphx.GSEdgeTriplet
import org.apache.spark.graphx.{Edge, EdgeTriplet, VertexId}

import scala.reflect.ClassTag

trait EdgeManager[VD,ED] {
  def iterator(startLid: Long, endLid: Long): Iterator[Edge[ED]]

  def getPartialEdgeNum(startLid: Long, endLid: Long): Long

  def getTotalEdgeNum: Long

  def iterateOnEdgesParallel[MSG](tid: Int, srcLid: Long, context: GSEdgeTriplet[VD, ED],
                                  msgSender: EdgeTriplet[VD, ED] => Iterator[(VertexId, MSG)], outMessageCache: MessageStore[MSG]): Unit

  def withNewEdgeData[ED2 : ClassTag](newEdgeData: Array[ED2], startLid: Long, endLid: Long): EdgeManager[VD, ED2]
}
