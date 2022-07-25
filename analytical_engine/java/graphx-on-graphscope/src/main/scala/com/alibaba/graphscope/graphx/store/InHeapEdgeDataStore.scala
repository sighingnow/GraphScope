package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.utils.GrapeUtils

import scala.reflect.ClassTag

class InHeapEdgeDataStore[ED: ClassTag](length : Int, client : VineyardClient, numSplit : Int, val edataArry : Array[ED], val oeOffsetToEid : Array[Long]) extends InHeapDataStore[ED](length,client,numSplit,edataArry){

  override def getData(offset : Int) : ED = {
    val eid = oeOffsetToEid(offset).toInt
    array(eid)
  }

  override def setData(offset : Int, value : ED) : Unit = {
    val eid = oeOffsetToEid(offset).toInt
    array(eid) = value
  }

  override def mapToNew[T2 : ClassTag]: DataStore[T2] = {
    if (!GrapeUtils.isPrimitive[T2]) {
      new InHeapEdgeDataStore[T2](length, client, numSplit, new Array[T2](length), oeOffsetToEid)
    }
    else {
      new OffHeapEdgeDataStore[T2](length,client,numSplit,oeOffsetToEid)
    }
  }
}
