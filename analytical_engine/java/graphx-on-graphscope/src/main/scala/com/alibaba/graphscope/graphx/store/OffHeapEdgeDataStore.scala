package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.{EdgeDataBuilder, VineyardArrayBuilder, VineyardClient}
import com.alibaba.graphscope.graphx.utils.{EIDAccessor, GrapeUtils, ScalaFFIFactory}

import scala.reflect.ClassTag

class OffHeapEdgeDataStore[ED: ClassTag](val length : Int, val client : VineyardClient, numSplit : Int, val eidAccessor: EIDAccessor,val edataBuilder: EdgeDataBuilder[Long, ED]) extends AbstractDataStore[ED](numSplit) with EdgeDataStore[ED]{
  require(GrapeUtils.isPrimitive[ED])
  val arrayBuilder: VineyardArrayBuilder[ED] = edataBuilder.getArrayBuilder

  override def size: Int = length

  override def getData(offset: Int): ED = {
    val eid = eidAccessor.getEid(offset).toInt
    arrayBuilder.get(eid)
  }

  override def setData(offset: Int, ed: ED): Unit = {
    val eid = eidAccessor.getEid(offset).toInt
    arrayBuilder.set(eid, ed)
  }

  override def mapToNew[T2 : ClassTag]: DataStore[T2] = {
    if (!GrapeUtils.isPrimitive[T2]) {
      new InHeapEdgeDataStore[T2](length, client, numSplit, new Array[T2](length), eidAccessor)
    }
    else {
      new OffHeapEdgeDataStore[T2](length,client,numSplit,eidAccessor,ScalaFFIFactory.newEdgeDataBuilder[T2](client,length))
    }
  }

  override def getWithEID(eid: Int): ED = arrayBuilder.get(eid)

  override def setWithEID(ind: Int, ed : ED): Unit = arrayBuilder.set(ind, ed)
}
