package com.alibaba.graphscope.graphx.store

import scala.reflect.ClassTag

trait VertexDataStore[VD] extends Serializable {
  def size : Int
  def getData(lid: Int) : VD
  def setData(lid : Int, vd : VD) : Unit
  def vineyardID : Long

  /** create a new store from current, all the same except for vertex data type */
  def create[VD2 : ClassTag] : VertexDataStore[VD2]

  def create[VD2 : ClassTag](newArr : Array[VD2]) : VertexDataStore[VD2]

  def getOrCreate[VD2: ClassTag] : VertexDataStore[VD2]
}
