package org.apache.spark.graphx.impl.partition.data

import com.alibaba.graphscope.graphx.{VertexData, VertexDataBuilder, VineyardClient}
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.utils.ScalaFFIFactory
import org.apache.spark.internal.Logging

import java.io.ObjectOutputStream
import scala.reflect.ClassTag

class InHeapVertexDataStore[VD: ClassTag](val vdArray : PrimitiveArray[VD], val client : VineyardClient) extends VertexDataStore [VD] with Logging{

  var vertexDataV6d: VertexData[Long, VD] = null.asInstanceOf[VertexData[Long,VD]]
  override def size: Long = vdArray.size()

  @inline
  override def getData(lid: Long): VD = vdArray.get(lid)

  override def vineyardID: Long = {
    if (vertexDataV6d == null) {
      if (GrapeUtils.getRuntimeClass[VD].equals(classOf[Long]) || GrapeUtils.getRuntimeClass[VD].equals(classOf[Double]) || GrapeUtils.getRuntimeClass[VD].equals(classOf[Int])){
        val newVdataBuilder = ScalaFFIFactory.newVertexDataBuilder[VD]()
        val arrowArrayBuilder = ScalaFFIFactory.newArrowArrayBuilder[VD](GrapeUtils.getRuntimeClass[VD].asInstanceOf[Class[VD]])
        arrowArrayBuilder.reserve(size)
        var i = 0
        while (i < size) {
          arrowArrayBuilder.unsafeAppend(getData(i))
          i += 1
        }
        newVdataBuilder.init(arrowArrayBuilder)
        vertexDataV6d = newVdataBuilder.seal(client).get()
      }
      else {
        val ffiByteVectorOutput = new FFIByteVectorOutputStream()
        val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
        var i = 0
        val limit = size
        while (i < limit){
          objectOutputStream.writeObject(getData(i))
          i += 1
        }
        objectOutputStream.flush()
        ffiByteVectorOutput.finishSetting()
        val writenBytes = ffiByteVectorOutput.bytesWriten()
        log.info(s"write vertex data ${limit} of type ${GrapeUtils.getRuntimeClass[VD].getName}, writen bytes ${writenBytes}")
        val newVdataBuilder = ScalaFFIFactory.newStringVertexDataBuilder().asInstanceOf[VertexDataBuilder[Long,VD]]
        newVdataBuilder.init(ffiByteVectorOutput.getVector)
        vertexDataV6d = newVdataBuilder.seal(client).get()
      }
    }
    vertexDataV6d.id()
  }

  @inline
  override def setData(lid: Long, vd: VD): Unit = vdArray.set(lid, vd)
}
