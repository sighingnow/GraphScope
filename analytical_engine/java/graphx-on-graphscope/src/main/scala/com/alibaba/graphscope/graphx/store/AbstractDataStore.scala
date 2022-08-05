package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.utils.{BitSetWithOffset, GrapeUtils}
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import scala.reflect.ClassTag

abstract class AbstractDataStore[T : ClassTag](var numSplit : Int) extends DataStore[T] with Logging{
  var resultArray : DataStore[_] = null.asInstanceOf[DataStore[_]]
  val count = new AtomicInteger(0)
  val lock = new ReentrantLock
  var clzObj = null.asInstanceOf[Class[_]]

  def setNumSplit(split : Int) : Unit = {
    this.numSplit = split
    count.set(0)
  }

  override def getOrCreate[T2: ClassTag](pid : Int): AbstractDataStore[T2] = {
//    if (resultArray == null || count.get() == 0){
//      count.synchronized {
//        if (resultArray == null || count.get() == 0) {
//          resultArray = mapToNew[T2]
//          log.info(s"pid ${pid} creating new result array ${resultArray} num split ${numSplit}, count ${count.get()}")
//          count.set(numSplit)
//        }
//      }
//    }
//    log.info(s"pid ${pid} cur count is ${count.get()}, dec and get")
//    count.decrementAndGet()
//    resultArray.asInstanceOf[AbstractDataStore[T2]]
    lock.lock()
    try {
      if (count.get() == 0){
        resultArray = mapToNew[T2]
        count.set(numSplit - 1)
        clzObj = GrapeUtils.getRuntimeClass[T2]
        log.info(s"pid ${pid} creating new result array ${resultArray} num split ${numSplit}, count ${count.get()}, clz ${clzObj.getName}")
      }
      else {
        require(clzObj.equals(GrapeUtils.getRuntimeClass[T2]), s"clz neq ${clzObj.getName}, ${GrapeUtils.getRuntimeClass[T2]}")
        log.info(s"pid ${pid} got num split ${numSplit}, count ${count.get()}, res ${resultArray}")
        count.decrementAndGet()
      }
      resultArray.asInstanceOf[AbstractDataStore[T2]]
    }
    finally {
      lock.unlock()
    }
  }
}
