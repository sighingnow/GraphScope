package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.{EdgeDataBuilder, VineyardArrayBuilder, VineyardClient}
import com.alibaba.graphscope.graphx.utils.{GrapeUtils, ScalaFFIFactory}

import scala.reflect.ClassTag

class OffHeapEdgeDataStore[ED: ClassTag](val length : Int, val client : VineyardClient, numSplit : Int, val oeOffsetToEid : Array[Long]) extends AbstractDataStore[ED](numSplit){
  require(GrapeUtils.isPrimitive[ED])
  val edataBuilder: EdgeDataBuilder[Long, ED] = ScalaFFIFactory.newEdgeDataBuilder[ED](client,length)
  val arrayBuilder: VineyardArrayBuilder[ED] = edataBuilder.getArrayBuilder

  override def size: Int = length

  override def getData(offset: Int): ED = {
    val eid = oeOffsetToEid(offset).toInt
    arrayBuilder.get(eid)
  }

  override def setData(offset: Int, ed: ED): Unit = {
    val eid = oeOffsetToEid(offset).toInt
    arrayBuilder.set(eid, ed)
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
