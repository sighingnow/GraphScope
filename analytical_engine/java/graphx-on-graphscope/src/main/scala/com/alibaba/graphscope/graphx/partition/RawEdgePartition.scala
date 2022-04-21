package com.alibaba.graphscope.graphx.partition

import org.apache.spark.internal.Logging

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class RawEdgePartition[@specialized(Char,Long,Int,Double) OID: ClassTag, VID: ClassTag, ED: ClassTag]
(srcOids : Array[OID], dstOids : Array[OID], eDatas : Array[ED]) extends Serializable with Logging{
  /** No-arg constructor for serialization. */
  private def this() = this(null, null, null)

  log.info(s"Creating raw edge Partition with size ${srcOids.length} ${dstOids.length}, ${eDatas.length}")

  def size = srcOids.length
}

class RawEdgePartitionBuilder[@specialized(Long, Int, Double) OID: ClassTag, VID : ClassTag, ED: ClassTag]{
  private val srcOidBuffer = new ArrayBuffer[OID]
  private val dstOidBuffer = new ArrayBuffer[OID]
  private val edataBuffer = new ArrayBuffer[ED]

  def add(src : OID, dst : OID, edata : ED): Unit ={
    srcOidBuffer += src
    dstOidBuffer += dst
    edataBuffer += edata
  }

  def size = srcOidBuffer.size
  def toRawEdgePartition : RawEdgePartition[OID,VID, ED] = {
    new RawEdgePartition[OID,VID,ED](srcOidBuffer.toArray, dstOidBuffer.toArray, edataBuffer.toArray)
  }

}
