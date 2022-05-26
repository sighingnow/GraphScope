package com.alibaba.graphscope.graphx

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream
import com.alibaba.graphscope.stdcxx.{FFIIntVector, FFIIntVectorFactory}
import org.apache.spark.graphx.utils.ScalaFFIFactory
import org.apache.spark.internal.Logging

import java.io.ObjectOutputStream

object ComplexTypeTest extends Logging{
  def main(args: Array[String]): Unit = {
    System.loadLibrary("grape-jni")
    val client = FFITypeFactory.getFactory(classOf[VineyardClient]).asInstanceOf[VineyardClient.Factory].create()
    val ffiByteString = FFITypeFactory.newByteString()
    ffiByteString.copyFrom("/tmp/vineyard.sock")
    client.connect(ffiByteString)
    log.info("vineyard connected");
    val ffiByteVectorOutput = new FFIByteVectorOutputStream()
    val ffiOffset = FFIIntVectorFactory.INSTANCE.create().asInstanceOf[FFIIntVector]
    val array = Array((1,3),(3,4))
    ffiOffset.resize(array.length)
    ffiOffset.touch()
    val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
    var i = 0
    val limit = array.length
    var prevBytesWritten = 0
    while (i < limit){
      objectOutputStream.writeObject(array(i))
      i += 1
      ffiOffset.set(i, ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten)
      prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
      log.info(s"Writing element ${i}: ${args(i).toString} cost ${ffiOffset.get(i)} bytes")
    }
    objectOutputStream.flush()
    ffiByteVectorOutput.finishSetting()
    val writenBytes = ffiByteVectorOutput.bytesWriten()
    log.info(s"write vertex data ${limit} of type ${array(0).getClass.getName}, writen bytes ${writenBytes}")
    val newVdataBuilder = ScalaFFIFactory.newStringVertexDataBuilder()
    newVdataBuilder.init(array.length, ffiByteVectorOutput.getVector, ffiOffset)
    val vertexDataV6dId = newVdataBuilder.seal(client).get().id()
    log.info(s"Got vertexdata id ${vertexDataV6dId}")
  }

}
