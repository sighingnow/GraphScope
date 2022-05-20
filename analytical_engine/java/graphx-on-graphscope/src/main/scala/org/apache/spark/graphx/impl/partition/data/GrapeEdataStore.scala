package org.apache.spark.graphx.impl.partition.data

import com.alibaba.graphscope.graphx.GraphXCSR

import scala.reflect.ClassTag

class GrapeEdataStore[ED: ClassTag](val csr : GraphXCSR[Long,ED]) extends EdgeDataStore[ED]{
  val array = csr.getEdataArray
  override def size: Long = array.getLength

  override def getData(eid: Long): ED = array.get(eid)
}
