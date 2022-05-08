package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.stdcxx.StdVector
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import org.apache.spark.graphx.impl.partition.VertexShuffle.{INIT_SIZE, vectorFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.PrimitiveVector


class VertexShuffle(val dstPid : Int, val fromPid: Int, array : Array[Long])extends Logging  with Serializable{
  def size() : Int = array.size

  def toVector() : StdVector[Long] = {
    val vector = vectorFactory.create()
    vector.resize(size())
    var i = 0;
    log.info(s"Writing vertex shuffle to vector ${vector} size ${size()}")
    while (i < array.size){
      vector.set(i, array(i))
      i += 1
    }
    vector
  }
}

class VertexShuffleBuilder (val dstPid : Int, val fromPid: Int) extends Logging {
  @transient val oidArray = new PrimitiveVector[Long](INIT_SIZE)

  def addOid(oid : Long) : Unit = oidArray.+=(oid)

  def finish() : VertexShuffle = {
    new VertexShuffle(dstPid, fromPid, oidArray.trim().array)
  }
}
object VertexShuffle{
  val INIT_SIZE = 4;
  val vectorFactory : StdVector.Factory[Long] = FFITypeFactoryhelper.getStdVectorFactory("std::vector<int64_t>").asInstanceOf[StdVector.Factory[Long]]
}
