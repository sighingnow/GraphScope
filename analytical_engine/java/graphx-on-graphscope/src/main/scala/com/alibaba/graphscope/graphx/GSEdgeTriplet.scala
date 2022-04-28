package com.alibaba.graphscope.graphx

import org.apache.spark.graphx.EdgeTriplet

trait GSEdgeTriplet[VD,ED] extends EdgeTriplet[VD,ED]{
  def setSrcOid(srcId : Long, srcAttr : VD): Unit
  def setDstOid(dstId : Long, dstAttr : VD, edgeAttr : ED): Unit
}
class GSEdgeTripletImpl[VD,ED] extends GSEdgeTriplet[VD,ED]{

  override def setSrcOid(srcId : Long, srcAttr : VD): Unit ={
    this.srcId = srcId
    this.srcAttr = srcAttr
  }

  override def setDstOid(dstId : Long, dstAttr : VD, edgeAttr : ED): Unit ={
    this.dstId = dstId;
    this.dstAttr = dstAttr
    this.attr = edgeAttr
  }
}

class ReverseGSEdgeTripletImpl[VD,ED] extends GSEdgeTriplet[VD,ED]{

  override def setSrcOid(srcId : Long, srcAttr : VD): Unit ={
    this.dstId = srcId
    this.dstAttr = srcAttr
  }

  override def setDstOid(dstId : Long, dstAttr : VD, edgeAttr : ED): Unit ={
    this.srcId = dstId;
    this.srcAttr = dstAttr
    this.attr = edgeAttr
  }
}
