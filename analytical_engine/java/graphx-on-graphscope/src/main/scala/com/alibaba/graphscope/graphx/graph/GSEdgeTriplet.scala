package com.alibaba.graphscope.graphx.graph

import org.apache.spark.graphx.EdgeTriplet

abstract class GSEdgeTriplet[VD,ED] extends EdgeTriplet[VD,ED]{
  var eid : Long = -1
  var offset : Long = -1;
  var srcLid : Long = -1;
  var dstLid : Long = -1;

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
class GSEdgeTripletImpl[@specialized(Long,Int,Double)VD, @specialized(Long,Int,Double)ED] extends GSEdgeTriplet[VD,ED]{

  def setSrcLid(srcLid : Long) : Unit = {
    this.srcLid = srcLid;
  }
  def setDstLid(dstLid : Long) : Unit = {
    this.dstLid = dstLid;
  }

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
