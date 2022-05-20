package org.apache.spark.graphx.impl.partition.data

trait VertexDataStore[VD] {
  def size : Long
  def getData(lid: Long) : VD
  def vineyardID : Long
}
