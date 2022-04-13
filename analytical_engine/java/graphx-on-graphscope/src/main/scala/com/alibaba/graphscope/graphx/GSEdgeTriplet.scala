package com.alibaba.graphscope.graphx

import org.apache.spark.graphx.EdgeTriplet

class GSEdgeTriplet[VD,ED] extends EdgeTriplet[VD,ED]{

  def setSrcOid(srcId : Long, srcAttr : VD): Unit ={
    this.srcId = srcId
    this.srcAttr = srcAttr
  }

  def setDstOid(dstId : Long, dstAttr : VD, edgeAttr : ED): Unit ={
    this.dstId = dstId;
    this.dstAttr = dstAttr
    this.attr = edgeAttr
  }
}
