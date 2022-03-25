package com.alibaba.graphscope.example

import com.alibaba.graphscope.app.GraphXAppBase
import org.apache.spark.graphx.{EdgeTriplet, VertexId}

/**
 * vd = long,
 * ed = long,
 * msg_t = long
 */
class SSSP extends GraphXAppBase[Long,Long,Long]{
  override def vprog(): (VertexId, Long, Long) => Long = {
    (id, dist, newDist) => math.min(dist, newDist)
  }

  override def sendMsg(): EdgeTriplet[Long, Long] => Iterator[(VertexId, Long)] = {
    triplet => {  // Send Message
      if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
        Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
      } else {
        Iterator.empty
      }
    }
  }

  override def mergeMsg(): (Long, Long) => Long = {
    (a, b) => math.min(a, b)
  }
}
