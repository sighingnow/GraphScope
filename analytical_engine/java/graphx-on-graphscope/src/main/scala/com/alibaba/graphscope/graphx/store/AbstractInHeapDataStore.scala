package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.utils.{BitSetWithOffset, GrapeUtils}
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

abstract class AbstractInHeapDataStore[@specialized(Long,Double,Int) VD: ClassTag](val length : Int, numSplit : Int, val array : Array[VD]) extends AbstractDataStore[VD](numSplit) with Logging {

  def this(length : Int,  numSplit : Int) = {
    this(length, numSplit,new Array[VD](length))
  }

  override def size: Int = array.length

  @inline
  override def getData(lid: Int): VD = array(lid)

  @inline
  override def setData(lid: Int, vd: VD): Unit = array(lid) = vd

//  override def mapToNew[T2 : ClassTag]: DataStore[T2] = new InHeapDataStore[T2](length,client,numSplit,new Array[T2](length))
}

