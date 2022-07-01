package com.alibaba.graphscope.graphx.store

trait VertexDataStore[VD] {
  def size : Long
  def getData(lid: Long) : VD
  def setData(lid : Long, vd : VD) : Unit
  def vineyardID : Long
}
