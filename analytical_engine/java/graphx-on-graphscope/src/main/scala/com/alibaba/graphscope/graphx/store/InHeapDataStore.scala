package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.utils.GrapeUtils
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

class InHeapDataStore[@specialized(Long,Double,Int) VD: ClassTag](val length : Int, val client : VineyardClient, numSplit : Int, val array : Array[VD]) extends AbstractDataStore[VD](numSplit) with Logging {

  def this(length : Int, client : VineyardClient, numSplit : Int) = {
    this(length,client, numSplit,new Array[VD](length))
  }

  override def size: Int = array.length

  @inline
  override def getData(lid: Int): VD = array(lid)

  @inline
  override def setData(lid: Int, vd: VD): Unit = array(lid) = vd


  override def toString: String = {
    "InHeapDataStore@(length=" + length + ",type=" + GrapeUtils.getRuntimeClass[VD].getSimpleName + ")"
  }

  override def mapToNew[T2 : ClassTag]: DataStore[T2] = new InHeapDataStore[T2](length,client,numSplit,new Array[T2](length))
}

