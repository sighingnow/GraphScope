package org.apache.spark.graphx.impl.partition

import org.apache.spark.graphx.Edge
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class EdgeShuffle[ED : ClassTag](val fromPid : Int,
                                 val dstPid: Int,
                                 val oids : OpenHashSet[Long], // all oids belong to dstPid
                                 val srcs : Array[Long],
                                 val dsts : Array[Long], val attrs: Array[ED]) extends Serializable {
  require(srcs.length == dsts.length)

  def size() : Long = srcs.length

  override def toString: String = "EdgeShuffle:{from "+ fromPid +",to "+ dstPid + ", oids:"+ oids.size + ",srcs: " + srcs.length + ",dsts: " + dsts.length + ",attrs:" + attrs.length;
}

class EdgeShuffleReceived[ED: ClassTag](val selfPid : Int) extends Logging{
  val fromPid2Shuffle = new ArrayBuffer[EdgeShuffle[ED]]

  def add(edgeShuffle: EdgeShuffle[ED]): Unit ={
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

//  def iterator() : Iterator[Edge[ED]] = {
//    new Iterator[Edge[ED]]{
//      var curPid = 0
//      var curInd = 0
//      var curShuffle :EdgeShuffle[ED] = fromPid2Shuffle(curPid)
//      val edge = new Edge[ED](0,0)
//      override def hasNext: Boolean = {
//          if (curInd >= curShuffle.size()){
//            curPid += 1
//	          var flag = true
//            while (curPid < numPartitions && flag){
//              curShuffle = fromPid2Shuffle(curPid)
//              if (curShuffle.size() > 0){
//  		          flag = false
//              }
//	            else {
//                 curPid += 1
//	            }
//            }
//            if (curPid >= numPartitions) return false
//            curInd = 0
//          }
//          true
//      }
//
//      override def next(): Edge[ED] = {
//        edge.srcId = curShuffle.srcs(curInd)
//        edge.dstId = curShuffle.dsts(curInd)
//        edge.attr = curShuffle.attrs(curInd)
//        curInd += 1
//        edge
//      }
//    }
//  }
}
