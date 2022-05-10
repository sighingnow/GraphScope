package org.apache.spark.graphx.impl.partition

import scala.reflect.ClassTag

class EdgeShuffle[ED : ClassTag](val fromPid : Int, val dstPid: Int, val srcs : Array[Long], val dsts : Array[Long], val attrs: Array[ED]) extends Serializable {
  require(srcs.length == dsts.length)

  def size() : Long = srcs.length
}

class EdgeShuffleReceived[ED: ClassTag](val numPartitions : Int, val selfPid : Int){
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
}
