package com.alibaba.graphscope.graphx.shuffle

import com.alibaba.graphscope.graphx.shuffle.EdgeShuffle.openHashSetToArray
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class EdgeShuffle[VD : ClassTag,ED : ClassTag](val fromPid : Int,
                                 val dstPid: Int,
                                 val oids : Array[Long], // all oids belong to dstPid
                                 val srcs : Array[Long],
                                 val dsts : Array[Long], val attrs: Array[ED],
                                 val vertexAttrs : Array[VD] = null) extends Serializable {
  def this(fromPid : Int, dstPid : Int, oids : OpenHashSet[Long], srcs : Array[Long], dsts : Array[Long], edgeAttrs : Array[ED]) = {
    this(fromPid,dstPid,openHashSetToArray(oids), srcs, dsts, edgeAttrs)
  }
  require(srcs.length == dsts.length)

  def size() : Long = srcs.length

  override def toString: String = "EdgeShuffle:{from "+ fromPid +",to "+ dstPid + ", oids:"+ oids.size + ",srcs: " + srcs.length + ",dsts: " + dsts.length + ",attrs:" + attrs.length;
}

object EdgeShuffle{
  def openHashSetToArray(value: OpenHashSet[Long]): Array[Long] ={
    val res = new Array[Long](value.size)
    val iter = value.iterator
    var i = 0
    while (iter.hasNext){
      res(i) = iter.next()
      i += 1
    }
    require(i == res.size)
    res
  }
}

class EdgeShuffleReceived[ED: ClassTag](val selfPid : Int) extends Logging{
  val fromPid2Shuffle = new ArrayBuffer[EdgeShuffle[_,ED]]

  def add(edgeShuffle: EdgeShuffle[_,ED]): Unit = {
    fromPid2Shuffle.+=(edgeShuffle)
  }

  def getArrays: (Array[Array[Long]], Array[Array[Long]], Array[Array[ED]]) = {
    val size = fromPid2Shuffle.size
    val srcArrays = new Array[Array[Long]](size)
    val dstArrays = new Array[Array[Long]](size)
    val attrArrays = new Array[Array[ED]](size)
    var i = 0
    while (i < size){
      srcArrays(i) = fromPid2Shuffle(i).srcs
      dstArrays(i) = fromPid2Shuffle(i).dsts
      attrArrays(i) = fromPid2Shuffle(i).attrs
      i += 1
    }
    (srcArrays,dstArrays,attrArrays)
  }

  /**
   * How many edges in received by us
   */
  def totalSize() : Long = {
    var i = 0;
    var res = 0L;
    while (i < fromPid2Shuffle.size){
      res += fromPid2Shuffle(i).size()
      i += 1
    }
    res
  }

  override def toString: String ={
    var res = s"EdgeShuffleReceived @Partition${selfPid}: "
    for (shuffle <- fromPid2Shuffle){
      res += s"(from ${shuffle.fromPid}, receive size ${shuffle.srcs.length});"
    }
    res
  }
}

object EdgeShuffleReceived{
  var data: EdgeShuffleReceived[_] = null.asInstanceOf[EdgeShuffleReceived[_]]
  def set(in : EdgeShuffleReceived[_]): Unit = {
    if (data == null){
      data = in
    }
    else {
      throw new IllegalStateException(s"data has been set to ${data}")
    }
  }
}
