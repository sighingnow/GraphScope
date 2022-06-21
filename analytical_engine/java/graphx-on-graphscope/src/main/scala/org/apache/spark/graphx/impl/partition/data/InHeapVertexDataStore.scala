package org.apache.spark.graphx.impl.partition.data

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream
import com.alibaba.graphscope.stdcxx.{FFIIntVector, FFIIntVectorFactory}
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.utils.ScalaFFIFactory
import org.apache.spark.internal.Logging

import java.io.ObjectOutputStream
import scala.reflect.ClassTag

class InHeapVertexDataStore[@specialized(Long,Double,Int) VD: ClassTag](val vdArray : PrimitiveArray[VD], val client : VineyardClient) extends VertexDataStore [VD] with Logging {

  var vertexDataV6dId: Long = 0L
  override def size: Long = vdArray.size()

  @inline
  override def getData(lid: Long): VD = vdArray.get(lid)

  override def vineyardID: Long = {
    if (vertexDataV6dId == 0) {
      vertexDataV6dId = GrapeUtils.array2ArrowArray[VD](vdArray, client)
    }
    vertexDataV6dId
  }

  @inline
  override def setData(lid: Long, vd: VD): Unit = vdArray.set(lid, vd)
}
