package com.alibaba.graphscope.graphx.store

import scala.reflect.ClassTag

trait DataStore[T] extends Serializable {
  def size : Int
  def getData(lid: Int) : T
  def setData(lid : Int, vd : T) : Unit
  def getOrCreate[T2: ClassTag] : DataStore[T2]
  //create a new object from myself.
  def mapToNew[T2 : ClassTag] : DataStore[T2]
}
