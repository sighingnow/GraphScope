package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.store.InHeapVertexDataStore.log
import org.apache.spark.internal.Logging

import java.util.concurrent.LinkedBlockingQueue

object EdgeStore extends Logging {
  val map = new java.util.HashMap[Int,LinkedBlockingQueue[AbstractDataStore[_]]]

  def enqueue(pid : Int, inHeapVertexDataStore: AbstractDataStore[_]) : Unit = {
    if (!map.containsKey(pid)){
      map.put(pid, new LinkedBlockingQueue)
    }
    val q = map.get(pid)
    q.offer(inHeapVertexDataStore)
    log.info(s"offering ${inHeapVertexDataStore} to part ${pid}")
  }

  def dequeue(pid : Int) : AbstractDataStore[_] = {
    require(map.containsKey(pid), s"no queue available for ${pid}")
    val res = map.get(pid).take()
    log.info(s"pid ${pid} got res ${res}")
    res
  }

}
