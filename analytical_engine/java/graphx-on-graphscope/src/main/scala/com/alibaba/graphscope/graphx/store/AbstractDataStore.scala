package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.utils.BitSetWithOffset
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

abstract class AbstractDataStore[T : ClassTag](var numSplit : Int) extends DataStore[T] with Logging{
  var resultArray : DataStore[_] = null.asInstanceOf[DataStore[_]]
  val count = new AtomicInteger(numSplit)

  def setNumSplit(split : Int) : Unit = {
    this.numSplit = split
    count.set(split)
  }

  override def getOrCreate[T2: ClassTag](pid : Int): AbstractDataStore[T2] = {
    if (resultArray == null || count.get() == 0){
      count.synchronized {
        if (resultArray == null || count.get() == 0) {
          resultArray = mapToNew[T2]
          log.info(s"pid ${pid} creating new result array ${resultArray} num split ${numSplit}, count ${count.get()}")
          count.set(numSplit)
        }
      }
    }
    log.info(s"pid ${pid} cur count is ${count.get()}, dec and get")
    count.decrementAndGet()
    resultArray.asInstanceOf[AbstractDataStore[T2]]
  }
}
