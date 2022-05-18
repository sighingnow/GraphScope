package org.apache.spark.graphx.utils

import org.apache.spark.graphx.impl.partition.EdgeShuffle

/**
 * avoid map out by spark rpc env, we cache this data in memory,and later just get it.
 */
object EdgeShuffleToMe{
  private var pid : Int = -1;
  private var shuffle : EdgeShuffle[_] = null.asInstanceOf[EdgeShuffle[_]]
  def set(pid : Int, shuffle : EdgeShuffle[_]) = {
    require(pid == -1)
    require(shuffle == null)
    this.pid = pid
    this.shuffle = shuffle

  }

  def get(pid : Int) : EdgeShuffle[_] = {
    require(pid != -1 && this.pid == pid)
    shuffle
  }
}
