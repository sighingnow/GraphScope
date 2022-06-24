package org.apache.spark.graphx.test

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.serialization.{FFIByteVectorInputStream, FFIByteVectorOutputStream}
import com.alibaba.graphscope.stdcxx.{FFIByteVector, FFIIntVector, FFIIntVectorFactory}
import org.apache.spark.graphx.utils.ScalaFFIFactory
import org.apache.spark.internal.Logging

import java.io.{ObjectInputStream, ObjectOutputStream}

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
    val array = Array((1,3),(3,4),(12,13),(34,53))
    ffiOffset.resize(array.length)
    ffiOffset.touch()
    val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
    var i = 0
    val limit = array.length
    var prevBytesWritten = 0
    objectOutputStream.writeLong(4)
    while (i < limit){
      objectOutputStream.writeObject(array(i))
      ffiOffset.set(i, ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten)
      prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
      log.info(s"Writing element ${i}: ${array(i).toString} cost ${ffiOffset.get(i)} bytes")
      i += 1
    }
    objectOutputStream.flush()
    ffiByteVectorOutput.finishSetting()
    val writenBytes = ffiByteVectorOutput.bytesWriten()
    log.info(s"write vertex data ${limit} of type ${array(0).getClass.getName}, writen bytes ${writenBytes}")
    val newVdataBuilder = ScalaFFIFactory.newStringVertexDataBuilder()
    newVdataBuilder.init(array.length, ffiByteVectorOutput.getVector, ffiOffset)
    val vertexData = newVdataBuilder.seal(client).get()
    log.info(s"Got vertexdata id ${vertexData.id()}")
    val vector = vertexData.getVdataArray.getRawBytes
    val address = vector.getAddress
    val ffiByteVector = new FFIByteVector(address)
    val ffiInput = new FFIByteVectorInputStream(ffiByteVector)
    val objectInputStream = new ObjectInputStream(ffiInput)
    val len = objectInputStream.readLong()
    for (i <- 0 until len.toInt){
      log.info(s"Reading ${i} th obj")
      val tuple = objectInputStream.readObject().asInstanceOf[(Int,Int)]
      log.info(s"${tuple}")
    }
  }

}
