package com.alibaba.graphscope.graphx.utils

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream
import com.alibaba.graphscope.stdcxx.{FFIIntVector, FFIIntVectorFactory}
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.internal.Logging

import java.io.ObjectOutputStream
import java.lang.reflect.Method
import java.net.{InetAddress, UnknownHostException}
import scala.reflect.ClassTag

object GrapeUtils extends Logging{

  def class2Int(value: Class[_]): Int = {
    if (value.equals(classOf[java.lang.Long]) || value.equals(classOf[Long])) {
      4
    }
    else if (value.equals(classOf[java.lang.Integer]) || value.equals(classOf[Int])) {
      2
    }
    else if (value.equals(classOf[java.lang.Double]) || value.eq(classOf[Double])) {
      7
    }
    else throw new IllegalArgumentException(s"unexpected class ${value}")
  }
  def bytesForType[VD: ClassTag](value: Class[VD]): Int ={
    if (value.equals(classOf[Long]) || value.equals(classOf[Double]) || value.equals(classOf[java.lang.Long]) || value.equals(classOf[java.lang.Double])) 8
    else if (value.eq(classOf[Int]) || value.equals(classOf[Float])) 4
    else throw new IllegalStateException("Unrecognized : " + value.getName)
  }

  def classToStr(value : Class[_]) : String = {
    if (value.equals(classOf[java.lang.Long]) || value.equals(classOf[Long])) {
      "int64_t"
    }
    else if (value.equals(classOf[java.lang.Integer]) || value.equals(classOf[Int])) {
      "int32_t"
    }
    else if (value.equals(classOf[java.lang.Double]) || value.eq(classOf[Double])) {
      "double"
    }
    else {
      "std::string"
    }
  }

  def getMethodFromClass[T](clz : Class[T], name : String ,paramClasses : Class[_]) : Method = {
    val method = clz.getDeclaredMethod(name, paramClasses)
    method.setAccessible(true)
    require(method != null, "can not find method: " + name)
    method
  }

  def generateForeignFragName[VD: ClassTag, ED : ClassTag](vdClass : Class[VD], edClass : Class[ED]): String ={
    new StringBuilder().+("gs::ArrowProjectedFragment<int64_t,uint64_t,").+(classToStr(vdClass)).+(",").+(classToStr(edClass)).+(">")
  }

  def scalaClass2JavaClass[T: ClassTag](vdClass : Class[T]) : Class[_] = {
    if (vdClass.equals(classOf[Int])) {
      classOf[Integer];
    }
    else if (vdClass.equals(classOf[Long])) {
      classOf[java.lang.Long]
    }
    else if (vdClass.equals(classOf[Double])) {
      classOf[java.lang.Double]
    }
    else {
      throw new IllegalStateException("transform failed for " + vdClass.getName);
    }
  }

  def getRuntimeClass[T: ClassTag] = implicitly[ClassTag[T]].runtimeClass

  def isPrimitive[T : ClassTag] : Boolean = {
    val clz = getRuntimeClass[T]
    clz.equals(classOf[Double]) || clz.equals(classOf[Long]) || clz.equals(classOf[Int]) || clz.equals(classOf[Float])
  }

  @throws[UnknownHostException]
  def getSelfHostName = InetAddress.getLocalHost.getHostName

  def dedup(files: Array[String]): Array[String] = {
    val set = files.toSet
    set.toArray
  }

  /**
   * used to transform offset base edata store to eids based store.
   * @tparam T data type
   */
  def rearrangeArrayWithIndex[T : ClassTag](array : PrimitiveArray[T], index : PrimitiveArray[Long]) : PrimitiveArray[T] = {
    val len = array.size()
    require(index.size() == len, s"array size ${len} neq eids array ${index.size()}")
    val newArray = PrimitiveArray.create(getRuntimeClass[T], len).asInstanceOf[PrimitiveArray[T]]
    var i = 0
    while (i < len){
      newArray.set(i, array.get(index.get(i)))
      i += 1
    }
    newArray
  }

  /** true for build vertex array, false for build edge array */
  def array2ArrowArray[T : ClassTag](array : PrimitiveArray[T], client : VineyardClient, vertex : Boolean) : Long = {
    val size = array.size()
    if (GrapeUtils.getRuntimeClass[T].equals(classOf[Long])
      || GrapeUtils.getRuntimeClass[T].equals(classOf[Double])
      || GrapeUtils.getRuntimeClass[T].equals(classOf[Int])){

      val arrowArrayBuilder = ScalaFFIFactory.newArrowArrayBuilder[T](GrapeUtils.getRuntimeClass[T].asInstanceOf[Class[T]])
      arrowArrayBuilder.reserve(size)
      var i = 0
      while (i < size) {
        arrowArrayBuilder.unsafeAppend(array.get(i))
        i += 1
      }
      if (vertex){
        val newVdataBuilder = ScalaFFIFactory.newVertexDataBuilder[T]()
        newVdataBuilder.init(arrowArrayBuilder)
        newVdataBuilder.seal(client).get().id()
      }
      else {
        val newEdataBuilder = ScalaFFIFactory.newEdgeDataBuilder[T]()
        newEdataBuilder.init(arrowArrayBuilder)
        newEdataBuilder.seal(client).get().id()
      }
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
        objectOutputStream.writeObject(array.get(i))
        ffiOffset.set(i, ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten)
        prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
//        log.info(s"Writing element ${i}: ${array.get(i).toString} cost ${ffiOffset.get(i)} bytes")
        i += 1
      }
      objectOutputStream.flush()
      ffiByteVectorOutput.finishSetting()
      val writenBytes = ffiByteVectorOutput.bytesWriten()
      log.info(s"write data array ${limit} of type ${GrapeUtils.getRuntimeClass[T].getName}, writen bytes ${writenBytes}")
      if (vertex){
        val newVdataBuilder = ScalaFFIFactory.newStringVertexDataBuilder()
        newVdataBuilder.init(size, ffiByteVectorOutput.getVector, ffiOffset)
        newVdataBuilder.seal(client).get().id()
      }
      else {
        val newEdataBuilder = ScalaFFIFactory.newStringEdgeDataBuilder()
        newEdataBuilder.init(size, ffiByteVectorOutput.getVector, ffiOffset)
        newEdataBuilder.seal(client).get().id()
      }
    }
  }
}
