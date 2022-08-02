package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.utils.BitSetWithOffset
import com.alibaba.graphscope.utils.ThreadSafeBitSet

import scala.reflect.ClassTag

class InHeapVertexDataStore [@specialized(Long,Double,Int) VD: ClassTag](val ivnum : Int, length : Int, client : VineyardClient, numSplit : Int) extends AbstractInHeapDataStore [VD](length,client,numSplit){
  var globalActive : ThreadSafeBitSet = null.asInstanceOf[ThreadSafeBitSet]
  override def mapToNew[T2: ClassTag]: DataStore[T2] = {
    new InHeapVertexDataStore[T2](ivnum,length,client,numSplit)
  }

  def updateActiveSet(partActive : BitSetWithOffset) : Unit = {
    if (globalActive == null){
      count.synchronized{
        if (globalActive == null){
          globalActive = new ThreadSafeBitSet()
        }
      }
    }
//      log.info(s"before union ${globalActive.cardinality()}")
      var cur = partActive.nextSetBit(partActive.startBit)
      val endBit = partActive.endBit
      while (cur < endBit && cur >= 0) {
        globalActive.set(cur)
        cur = partActive.nextSetBit(cur + 1)
      }
//      log.info(s"after union ${globalActive.cardinality()}")
//    }
  }
}
