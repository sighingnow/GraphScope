package com.alibaba.graphscope.graphx

import com.alibaba.fastffi.FFITypeFactory
import org.apache.spark.graphx.utils.ScalaFFIFactory
import org.apache.spark.internal.Logging

object SimpleTypeTest extends Logging{
  def main(args: Array[String]): Unit = {
    System.loadLibrary("grape-jni")
    val client = FFITypeFactory.getFactory(classOf[VineyardClient]).asInstanceOf[VineyardClient.Factory].create()
    val ffiByteString = FFITypeFactory.newByteString()
    ffiByteString.copyFrom("/tmp/vineyard.sock")
    client.connect(ffiByteString)
    log.info("vineyard connected");
    val newVdataBuilder = ScalaFFIFactory.newVertexDataBuilder[Int]()
    newVdataBuilder.init(62586, 1)
    val vertexData = newVdataBuilder.seal(client).get()
    log.info(s"Got vertexdata id ${vertexData.id()}")
    log.info(s"get data ${vertexData.getData(0)}")
  }
}
