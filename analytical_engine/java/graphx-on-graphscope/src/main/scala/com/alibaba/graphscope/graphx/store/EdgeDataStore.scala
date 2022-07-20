package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.VineyardClient

import scala.reflect.ClassTag

class EdgeDataStore[ED: ClassTag]( length : Int, client : VineyardClient, numSplit : Int, val edataArry : Array[ED], val oeOffsetToEid : Array[Long]) extends InHeapDataStore[ED](length,client,numSplit,edataArry){

  def getOEData(offset : Int) : ED = {
    val eid = oeOffsetToEid(offset).toInt
    array(eid)
  }

  def setOEData(offset : Int, value : ED) : Unit = {
    val eid = oeOffsetToEid(offset).toInt
    array(eid) = value
  }

  def getEdata(eid : Int) : ED = {
    array(eid)
  }

  override def getOrCreate[VD2: ClassTag]: DataStore[VD2] = synchronized{
    if (resultArray == null || count.get() == 0){
      synchronized {
        //        log.info(s"creating result array of type ${GrapeUtils.getRuntimeClass[VD2].getSimpleName}")
        resultArray = new EdgeDataStore[VD2](length, client, numSplit, new Array[VD2](length),oeOffsetToEid).asInstanceOf[InHeapDataStore[_]]
        count.set(numSplit)
      }
    }
    //  log.info(s"using already exiting res array ${resultArray}")
    count.decrementAndGet()
    resultArray.asInstanceOf[DataStore[VD2]]
  }
}
