package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.utils.GrapeUtils
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class InHeapVertexDataStore[@specialized(Long,Double,Int) VD: ClassTag](val vdArray : PrimitiveArray[VD], val client : VineyardClient, val versionId : Int) extends VertexDataStore [VD] with Logging {

  var vertexDataV6dId: Long = 0L
  override def size: Long = vdArray.size()

  @inline
  override def getData(lid: Long): VD = vdArray.get(lid)

  override def vineyardID: Long = {
    if (vertexDataV6dId == 0) {
      vertexDataV6dId = GrapeUtils.array2ArrowArray[VD](vdArray, client,true)
    }
    vertexDataV6dId
  }

  @inline
  override def setData(lid: Long, vd: VD): Unit = vdArray.set(lid, vd)

  /**
   * Indicating the version of cur vertex data. used by edge partition to judge whether are left behind.
   * */
  override def version: Int = versionId

  override def withNewValues[VD2 : ClassTag](newArr : PrimitiveArray[VD2]) : VertexDataStore[VD2] = new InHeapVertexDataStore[VD2](newArr, client, versionId + 1)
}

