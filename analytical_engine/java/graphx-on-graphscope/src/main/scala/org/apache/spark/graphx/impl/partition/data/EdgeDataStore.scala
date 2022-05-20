package org.apache.spark.graphx.impl.partition.data

trait EdgeDataStore[ED] {
  def size : Long
  def getData(eid : Long) : ED
}
