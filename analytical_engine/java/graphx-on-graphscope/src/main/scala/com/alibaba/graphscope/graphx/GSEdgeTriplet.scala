package com.alibaba.graphscope.graphx

import org.apache.spark.graphx.EdgeTriplet

abstract class GSEdgeTriplet[VD,ED] extends EdgeTriplet[VD,ED]{
  var index : Long = -1
//  var eid : Long = -1
  def getSrcOid : Long = srcId
  def getSrcAttr : VD = srcAttr
  def getDstOid : Long = dstId
  def getDstAttr : VD = dstAttr
  def getAttr : ED = attr

  def setSrcOid(srcId : Long, srcAttr : VD): Unit
  def setSrcOid(srcId : Long): Unit
  def setDstOid(dstId : Long, dstAttr : VD): Unit
  def setDstOid(dst : Long): Unit
  def setAttr(edgeAttr: ED) : Unit
}
class GSEdgeTripletImpl[VD,ED] extends GSEdgeTriplet[VD,ED]{

  override def setSrcOid(srcId : Long, srcAttr : VD): Unit ={
    this.srcId = srcId
    this.srcAttr = srcAttr
  }

  override def setDstOid(dstId : Long, dstAttr : VD): Unit ={
    this.dstId = dstId;
    this.dstAttr = dstAttr
  }

  override def setSrcOid(srcId: Long): Unit = this.srcId = srcId

  override def setDstOid(dstId: Long): Unit = this.dstId = dstId

  override def setAttr(edgeAttr: ED): Unit = this.attr = edgeAttr

  override def toString(): String = "GSEdgeTripletImpl(" + "srcId=" +srcId +
    ",dstId=" +dstId + ",srcAttr=" + srcAttr + ",dstAttr=" + dstAttr + ",attr=" + attr + ")"
}

class ReverseGSEdgeTripletImpl[VD,ED] extends GSEdgeTriplet[VD,ED]{

  override def setSrcOid(srcId : Long, srcAttr : VD): Unit ={
    this.dstId = srcId
    this.dstAttr = srcAttr
  }

  override def setDstOid(dstId : Long, dstAttr : VD): Unit ={
    this.srcId = dstId;
    this.srcAttr = dstAttr
  }

  override def setSrcOid(srcId: Long): Unit = this.dstId = srcId

  override def setDstOid(srcId: Long): Unit = this.srcId = dstId

  override def setAttr(edgeAttr: ED): Unit = this.attr = edgeAttr

  override def toString(): String = "ReverseEdgeTripletImpl(" + "srcId=" +srcId +
    ",dstId=" +dstId + ",srcAttr=" + srcAttr + ",dstAttr=" + dstAttr + ",attr=" + attr + ")"
}
