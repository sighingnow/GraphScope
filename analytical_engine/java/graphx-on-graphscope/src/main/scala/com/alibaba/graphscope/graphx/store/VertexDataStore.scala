package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.utils.array.PrimitiveArray

import scala.reflect.ClassTag

trait VertexDataStore[VD] extends Serializable {
  def size : Long
  def getData(lid: Long) : VD
  def setData(lid : Long, vd : VD) : Unit
  def vineyardID : Long
  /**
   * Indicating the version of cur vertex data. used by edge partition to judge whether are left behind.
   *  */
  def version : Int

  def withNewValues[VD2 : ClassTag](newArr : PrimitiveArray[VD2]) : VertexDataStore[VD2]
}
