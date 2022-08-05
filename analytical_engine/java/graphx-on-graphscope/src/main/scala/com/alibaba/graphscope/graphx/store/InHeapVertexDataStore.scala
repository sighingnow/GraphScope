package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.store.EdgeStore.map
import com.alibaba.graphscope.graphx.utils.BitSetWithOffset
import com.alibaba.graphscope.utils.ThreadSafeBitSet
import org.apache.spark.internal.Logging

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable
import scala.reflect.ClassTag

class InHeapVertexDataStore [@specialized(Long,Double,Int) VD: ClassTag](val ivnum : Int, length : Int, numSplit : Int) extends AbstractInHeapDataStore [VD](length,numSplit){
  var globalActive : ThreadSafeBitSet = null.asInstanceOf[ThreadSafeBitSet]
  override def mapToNew[T2: ClassTag]: DataStore[T2] = {
    new InHeapVertexDataStore[T2](ivnum,length,numSplit)
  }

//  def updateActiveSet(partActive : BitSetWithOffset) : Unit = {
//    if (globalActive == null){
//      count.synchronized{
//        if (globalActive == null){
//          globalActive = new ThreadSafeBitSet()
//        }
//      }
//    }
//      log.info(s"before union ${globalActive.cardinality()}")
//      var cur = partActive.nextSetBit(partActive.startBit)
//      val endBit = partActive.endBit
//      while (cur < endBit && cur >= 0) {
//        globalActive.set(cur)
//        cur = partActive.nextSetBit(cur + 1)
//      }
////      log.info(s"after union ${globalActive.cardinality()}")
////    }
//  }
}

object InHeapVertexDataStore extends Logging{
  val map = new java.util.HashMap[Int,LinkedBlockingQueue[InHeapVertexDataStore[_]]]

  def enqueue(pid : Int, inHeapVertexDataStore: InHeapVertexDataStore[_]) : Unit = {
    createQueue(pid)
    val q = map.get(pid)
    q.offer(inHeapVertexDataStore)
    log.info(s"offering ${inHeapVertexDataStore} to part ${pid}")
  }

  def dequeue(pid : Int) : InHeapVertexDataStore[_] = {
    createQueue(pid)
    require(map.containsKey(pid), s"no queue available for ${pid}")
    val res = map.get(pid).take()
    log.info(s"pid ${pid} got res ${res}")
    res
  }

  def createQueue(pid : Int) : Unit = synchronized{
    if (!map.containsKey(pid)){
      map.put(pid, new LinkedBlockingQueue)
    }
  }
}