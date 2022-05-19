package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.graphx.{GraphXVertexMap, VertexData}
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

class GrapeVertexPartitionWithDataCache[VD : ClassTag](pid : Int, vm : GraphXVertexMap[Long,Long],
                                                       val newVertexData : PrimitiveArray[VD],
                                                       activeVertices : BitSet = null) extends  GrapeVertexPartition[VD](pid, vm, null, activeVertices){
  require(newVertexData.size() == vm.getVertexSize)

  override def getData(lid: Long): VD = {
    newVertexData.get(lid)
  }

}
