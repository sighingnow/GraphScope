package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.utils.{EIDAccessor, GrapeUtils, ScalaFFIFactory}

import scala.reflect.ClassTag

class InHeapEdgeDataStore[ED: ClassTag](length : Int, client : VineyardClient, numSplit : Int, val edataArry : Array[ED], eidAccessor : EIDAccessor) extends AbstractInHeapDataStore[ED](length,numSplit,edataArry) with EdgeDataStore[ED]{

  override def getData(offset : Int) : ED = {
    val eid = eidAccessor.getEid(offset).toInt
    array(eid)
  }

  override def setData(offset : Int, value : ED) : Unit = {
    val eid = eidAccessor.getEid(offset).toInt
    array(eid) = value
  }

  override def mapToNew[T2 : ClassTag]: DataStore[T2] = {
    if (!GrapeUtils.isPrimitive[T2]) {
      new InHeapEdgeDataStore[T2](length, client, numSplit, new Array[T2](length), eidAccessor)
    }
    else {
      new OffHeapEdgeDataStore[T2](length,client,numSplit,eidAccessor,ScalaFFIFactory.newEdgeDataBuilder[T2](client,length))
    }
  }

  override def getWithEID(eid: Int): ED = {
    array(eid)
  }

  override def setWithEID(ind: Int, ed: ED): Unit = {
    array(ind) = ed
  }
}
