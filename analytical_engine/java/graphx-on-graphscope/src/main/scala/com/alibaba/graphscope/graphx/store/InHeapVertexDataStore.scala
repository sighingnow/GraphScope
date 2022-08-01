package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.utils.BitSetWithOffset

import scala.reflect.ClassTag

class InHeapVertexDataStore [@specialized(Long,Double,Int) VD: ClassTag](val ivnum : Int, length : Int, client : VineyardClient, numSplit : Int) extends AbstractInHeapDataStore [VD](length,client,numSplit){
  var globalActive : BitSetWithOffset = null.asInstanceOf[BitSetWithOffset]
  override def mapToNew[T2: ClassTag]: DataStore[T2] = {
    new InHeapVertexDataStore[T2](ivnum,length,client,numSplit)
  }

  def updateActiveSet(partActive : BitSetWithOffset) : Unit = {
    if (globalActive == null){
      count.synchronized{
        if (globalActive == null){
          globalActive = new BitSetWithOffset(0,ivnum)
        }
      }
    }
    globalActive.synchronized{
      log.info(s"before union ${globalActive.cardinality()}")
      globalActive.union(partActive)
      log.info(s"after union ${globalActive.cardinality()}")
    }
  }
}
