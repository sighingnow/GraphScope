package org.apache.spark.graphx.impl.partition.data

trait VertexDataStore[VD] {
  def size : Long
  def getData(lid: Long) : VD
  def setData(lid : Long, vd : VD) : Unit
  def vineyardID : Long
}
