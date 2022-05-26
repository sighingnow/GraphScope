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

class InHeapVertexDataStore[VD: ClassTag](val vdArray : PrimitiveArray[VD], val client : VineyardClient) extends VertexDataStore [VD] with Logging{

  var vertexDataV6dId: Long = 0L
  override def size: Long = vdArray.size()

  @inline
  override def getData(lid: Long): VD = vdArray.get(lid)

  override def vineyardID: Long = {
    if (vertexDataV6dId == 0) {
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
        vertexDataV6dId = newVdataBuilder.seal(client).get().id()
      }
      else {
        val ffiByteVectorOutput = new FFIByteVectorOutputStream()
        val ffiOffset = FFIIntVectorFactory.INSTANCE.create().asInstanceOf[FFIIntVector]
        ffiOffset.resize(size)
        ffiOffset.touch()
        val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
        var i = 0
        val limit = size
        var prevBytesWritten = 0
        while (i < limit){
          objectOutputStream.writeObject(getData(i))
          ffiOffset.set(i, ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten)
          prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
          log.info(s"Writing element ${i}: ${getData(i).toString} cost ${ffiOffset.get(i)} bytes")
          i += 1
        }
        objectOutputStream.flush()
        ffiByteVectorOutput.finishSetting()
        val writenBytes = ffiByteVectorOutput.bytesWriten()
        log.info(s"write vertex data ${limit} of type ${GrapeUtils.getRuntimeClass[VD].getName}, writen bytes ${writenBytes}")
        val newVdataBuilder = ScalaFFIFactory.newStringVertexDataBuilder()
        newVdataBuilder.init(size, ffiByteVectorOutput.getVector, ffiOffset)
        vertexDataV6dId = newVdataBuilder.seal(client).get().id()
      }
    }
    vertexDataV6dId
  }

  @inline
  override def setData(lid: Long, vd: VD): Unit = vdArray.set(lid, vd)
}
