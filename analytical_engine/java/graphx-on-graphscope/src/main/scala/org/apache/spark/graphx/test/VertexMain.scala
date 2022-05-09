package org.apache.spark.graphx.test

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.graphx.{GrapeVertexPartition, GrapeVertexPartitionBuilder}
import org.apache.spark.graphx.impl.GrapeVertexPartitionWrapper
import org.apache.spark.graphx.impl.partition.VertexShuffle
import org.apache.spark.internal.Logging

object VertexMain extends Logging{
  def main(array: Array[String]) : Unit = {
    val vecFactory = VertexShuffle.vectorFactory
    val builderForeignName = "gs::VertexPartitionBuilder<int64_t,uint64_t,int32_t>"
    val partitionForeignName = "gs::VertexPartition<int64_t,uint64_t,int32_t>"
    val builderFactory = FFITypeFactory.getFactory(classOf[GrapeVertexPartitionBuilder[_,_,_]], builderForeignName).asInstanceOf[GrapeVertexPartitionBuilder.Factory[Long,Long,Int]]
    val partitionFactory = FFITypeFactory.getFactory(classOf[GrapeVertexPartition[_,_,_]], partitionForeignName).asInstanceOf[GrapeVertexPartition.Factory[Long,Long,Int]]

    log.info("got all factories")
    val builder = builderFactory.create()
    val grapeVertexPartition = partitionFactory.create()
    val vector = vecFactory.create()
    val vector2 = vecFactory.create()
    log.info("create all ffi pointer")

    vector.resize(3)
    vector.set(0, 1)
    vector.set(1,2);
    vector.set(2,3);
    builder.addVertex(vector, 0)

    vector2.resize(3)
    vector2.set(0, 4)
    vector2.set(1,5);
    vector2.set(2,6);
    builder.addVertex(vector2, 1)

    builder.Build(grapeVertexPartition, 1)

    val wrapper = new GrapeVertexPartitionWrapper[Int](0, 1, 0, 5, grapeVertexPartition)
    log.info(s"got wrapper ${wrapper}")
  }
}
