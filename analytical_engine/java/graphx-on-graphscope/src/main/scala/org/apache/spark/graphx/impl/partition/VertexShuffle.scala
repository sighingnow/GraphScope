package org.apache.spark.graphx.impl.partition

import com.alibaba.fastffi.{FFITypeFactory, FFIVector}
import com.alibaba.graphscope.stdcxx.StdVector
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import org.apache.spark.graphx.PartitionID
import org.apache.spark.graphx.impl.GrapeEdgePartitionWrapper
import org.apache.spark.graphx.impl.partition.VertexShuffle.{INIT_SIZE, vectorFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.PrimitiveVector


class VertexShuffle(val dstPid : Int, val fromPid: Int)extends Logging{
//  val oidArray = new Array[Long](INIT_SIZE)
  val oidArray = new PrimitiveVector[Long](INIT_SIZE)

  def addOid(oid : Long) : Unit = oidArray.+=(oid)

  def size() : Int = oidArray.size

  def toVector() : StdVector[Long] = {
    val vector = vectorFactory.create()
    vector.reserve(size())
    var i = 0;
    log.info(s"Writing vertex shuffle to vector ${vector} size ${size()}")
    while (i < oidArray.size){
      vector.set(i, oidArray(i))
      i += 1
    }
    vector
  }
}
object VertexShuffle{
  val INIT_SIZE = 4;
  val vectorFactory : StdVector.Factory[Long] = FFITypeFactoryhelper.getStdVectorFactory("std::vector<int64_t>").asInstanceOf[StdVector.Factory[Long]]
}
