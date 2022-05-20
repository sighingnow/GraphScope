package org.apache.spark.graphx.impl.partition.data

import com.alibaba.graphscope.graphx.{VertexData, VineyardClient}
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.utils.ScalaFFIFactory

import scala.reflect.ClassTag

class InHeapVertexDataStore[VD: ClassTag](val vdArray : PrimitiveArray[VD], val client : VineyardClient) extends VertexDataStore [VD]{

  var vertexDataV6d: VertexData[Long, VD] = null.asInstanceOf[VertexData[Long,VD]]
  override def size: Long = vdArray.size()

  override def getData(lid: Long): VD = vdArray.get(lid)

  override def vineyardID: Long = {
    if (vertexDataV6d == null){
      val newVdataBuilder = ScalaFFIFactory.newVertexDataBuilder[VD]()
      val arrowArrayBuilder = ScalaFFIFactory.newArrowArrayBuilder[VD](GrapeUtils.getRuntimeClass[VD].asInstanceOf[Class[VD]])
      arrowArrayBuilder.reserve(size)
      var i = 0
      while (i < size){
        arrowArrayBuilder.unsafeAppend(getData(i))
        i += 1
      }
      newVdataBuilder.init(arrowArrayBuilder)
      vertexDataV6d = newVdataBuilder.seal(client).get()
    }
    vertexDataV6d.id()
  }
}
