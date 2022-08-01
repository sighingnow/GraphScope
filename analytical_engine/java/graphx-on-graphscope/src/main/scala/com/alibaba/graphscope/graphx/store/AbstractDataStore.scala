package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.utils.BitSetWithOffset

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

abstract class AbstractDataStore[T : ClassTag](var numSplit : Int) extends DataStore [T]{
  var resultArray : DataStore[_] = null.asInstanceOf[DataStore[_]]
  val count = new AtomicInteger(numSplit)

  def setNumSplit(split : Int) : Unit = {
    this.numSplit = split
    count.set(split)
  }

  override def getOrCreate[T2: ClassTag]: AbstractDataStore[T2] = {
    if (resultArray == null || count.get() == 0){
      synchronized {
        //        log.info(s"creating result array of type ${GrapeUtils.getRuntimeClass[VD2].getSimpleName}")
        resultArray = mapToNew[T2]
        count.set(numSplit)
      }
    }
    count.decrementAndGet()
    resultArray.asInstanceOf[AbstractDataStore[T2]]
  }
}
