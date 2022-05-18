package org.apache.spark.graphx.utils

import org.apache.spark.graphx.impl.partition.EdgeShuffle
import org.apache.spark.internal.Logging

/**
 * avoid map out by spark rpc env, we cache this data in memory,and later just get it.
 */
object EdgeShuffleToMe extends Logging{
  private var pid : Int = -1;
  private var shuffle : EdgeShuffle[_] = null.asInstanceOf[EdgeShuffle[_]]
  def set(pid : Int, shuffle : EdgeShuffle[_]) = {
    require(this.pid == -1)
    require(this.shuffle == null)
    this.pid = pid
    this.shuffle = shuffle
    log.info(s"Seting edge shuffle ${shuffle} on Partition {pid}")  
  }

  def get(pid : Int) : EdgeShuffle[_] = {
    require(pid != -1 && this.pid == pid)
    shuffle
  }
}
