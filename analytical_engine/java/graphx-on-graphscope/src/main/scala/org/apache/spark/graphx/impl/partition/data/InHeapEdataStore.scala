package org.apache.spark.graphx.impl.partition.data

import com.alibaba.graphscope.utils.array.PrimitiveArray

import scala.reflect.ClassTag

class InHeapEdataStore[ED: ClassTag](val array : PrimitiveArray[ED]) extends EdgeDataStore [ED]{
  override def size: Long = array.size()

  override def getData(eid: Long): ED = array.get(eid)
}
