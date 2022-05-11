package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.stdcxx.StdVector
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class EdgeShuffle[ED : ClassTag](val fromPid : Int, val dstPid: Int, val srcs : Array[Long], val dsts : Array[Long], val attrs: Array[ED]) extends Serializable {
  require(srcs.length == dsts.length)

  def size() : Long = srcs.length
}

class EdgeShuffleReceived[ED: ClassTag](val numPartitions : Int, val selfPid : Int) extends Logging{
  val fromPid2Shuffle = new Array[EdgeShuffle[ED]](numPartitions)

  def set(ind : Int, edgeShuffle: EdgeShuffle[ED]): Unit ={
    if (fromPid2Shuffle(ind) != null){
      throw new IllegalStateException(s"shuffle already in at ${ind}")
    }
    fromPid2Shuffle(ind) = edgeShuffle
  }

  def get(ind : Int) : EdgeShuffle[ED]= {
    fromPid2Shuffle(ind)
  }

  /**
   * How many edges in received by us
   */
  def totalSize() : Long = {
    var i = 0;
    var res = 0L;
    while (i < numPartitions){
      res += fromPid2Shuffle(i).size()
      i += 1
    }
    res
  }

  override def toString: String ={
    var res = s"EdgeShuffleReceived by ${selfPid}: "
    for (shuffle <- fromPid2Shuffle){
      res += s"from ${shuffle.fromPid}, receive size ${shuffle.srcs.length};"
    }
    res
  }
  def feedToBuilder(srcOidBuilder : StdVector[Long], dstOidBuilder : StdVector[Long], edataBuilder : StdVector[ED]) : Unit = {
      log.info(s"start adding edges of size ${totalSize()}")
      var fromPid = 0
      val curPid = selfPid
      while (fromPid < numPartitions){
        val edgeShuffle = fromPid2Shuffle(fromPid)
        log.info(s"Partition ${curPid} receive num of shuffles ${edgeShuffle.size()} from ${edgeShuffle.fromPid} == ${fromPid}")
        var i = 0
        val limit = edgeShuffle.size()
        val srcArray = edgeShuffle.srcs
        val dstArray = edgeShuffle.dsts
        val attrArray = edgeShuffle.attrs
        while (i < limit){
          val srcId = srcArray(i)
          val dstId = dstArray(i)
          val attr = attrArray(i)
          srcOidBuilder.push_back(srcId)
          dstOidBuilder.push_back(dstId)
          edataBuilder.push_back(attr)
          i += 1
        }
        fromPid += 1
      }
      log.info(s"Partition ${curPid} finish process all edges.")
    }
}
