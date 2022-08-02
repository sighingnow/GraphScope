package com.alibaba.graphscope.graphx.utils

import com.alibaba.fastffi.impl.CXXStdString
import com.alibaba.graphscope.arrow.array.{ArrowArray, ArrowArrayBuilder}
import com.alibaba.graphscope.graphx.store.{AbstractDataStore, OffHeapEdgeDataStore}
import com.alibaba.graphscope.graphx.{EdgeData, StringEdgeData, StringEdgeDataBuilder, StringVertexData, VertexData, VineyardArrayBuilder, VineyardClient}
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream
import com.alibaba.graphscope.stdcxx.{FFIByteVector, FFIIntVector, FFIIntVectorFactory, StdVector}
import com.alibaba.graphscope.utils.{FFITypeFactoryhelper, ThreadSafeBitSet}
import com.alibaba.graphscope.utils.array.PrimitiveArray
import com.esotericsoftware.kryo.io.Output
import com.twitter.chill.Kryo
import org.apache.spark.internal.Logging

import java.io.ObjectOutputStream
import java.lang.reflect.Method
import java.net.{InetAddress, UnknownHostException}
import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

object GrapeUtils extends Logging{
  val BATCH_SIZE = 4096

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

  def fillPrimitiveArrowArrayBuilder[T : ClassTag](array: Array[T]) : ArrowArrayBuilder[T] = {
    val size = array.length
    val arrowArrayBuilder = ScalaFFIFactory.newArrowArrayBuilder[T](GrapeUtils.getRuntimeClass[T].asInstanceOf[Class[T]])
    arrowArrayBuilder.reserve(size)
    var i = 0
    while (i < size) {
      arrowArrayBuilder.unsafeAppend(array(i))
      i += 1
    }
    arrowArrayBuilder
  }
  def fillPrimitiveVector[T : ClassTag](array: Array[T], numThread : Int) : StdVector[T] = {
    val time0 = System.nanoTime()
    val size = array.length
    val vector = ScalaFFIFactory.newVector[T]
    vector.resize(size)
    val threadArray = new Array[Thread](numThread)
    val atomic = new AtomicInteger(0)
    for (i <- 0 until numThread){
      threadArray(i) = new Thread(){
        override def run(): Unit ={
          var flag = true
          while (flag){
            val begin = Math.min(atomic.getAndAdd(BATCH_SIZE), size)
            val end = Math.min(begin + BATCH_SIZE, size)
            if (begin >= end){
              flag = false
            }
            else {
              var i = begin
              while (i < end){
                vector.set(i, array(i))
                i += 1
              }
            }
          }
        }
      }
      threadArray(i).start()
    }
    for (i <- 0 until numThread){
      threadArray(i).join()
    }
    val time1 = System.nanoTime()
    log.info(s"Building primitive array size ${size} with num thread ${numThread} cost ${(time1 - time0)/1000000}ms")
    vector
  }
  def fillPrimitiveVineyardArray[T : ClassTag](array: Array[T], vineyardBuilder : VineyardArrayBuilder[T], numThread : Int) : Unit = {
    val time0 = System.nanoTime()
    val size = array.length
    val threadArray = new Array[Thread](numThread)
    val atomic = new AtomicInteger(0)
    for (i <- 0 until numThread){
      threadArray(i) = new Thread(){
        override def run(): Unit ={
          var flag = true
          while (flag){
            val begin = Math.min(atomic.getAndAdd(BATCH_SIZE), size)
            val end = Math.min(begin + BATCH_SIZE, size)
            if (begin >= end){
              flag = false
            }
            else {
              var i = begin
              while (i < end){
                vineyardBuilder.set(i, array(i))
                i += 1
              }
            }
          }
        }
      }
      threadArray(i).start()
    }
    for (i <- 0 until numThread){
      threadArray(i).join()
    }
    val time1 = System.nanoTime()
    log.info(s"Building primitive array size ${size} with num thread ${numThread} cost ${(time1 - time0)/1000000}ms")
  }

  def fillVertexStringArrowArray[T : ClassTag](array: Array[T],activeVertices : ThreadSafeBitSet) : (FFIByteVector, FFIIntVector) = {
    val size = array.length
    val ffiByteVectorOutput = new FFIByteVectorOutputStream()
//    val output = new Output(ffiByteVectorOutput)
    val ffiOffset = FFIIntVectorFactory.INSTANCE.create().asInstanceOf[FFIIntVector]
    ffiOffset.resize(size)
    ffiOffset.touch()
    val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
    var i = activeVertices.nextSetBit(0)
    val limit = size
    var prevBytesWritten = 0
    var nullCount = 0
    while (i < limit && i >= 0){
      if (array(i) == null){
        nullCount +=1
      }
      objectOutputStream.writeObject(array(i))
      ffiOffset.set(i, ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten)
      prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
      i += 1
    }
    log.info(s"total size ${size} null count ${nullCount}, active ${activeVertices.cardinality()}")
    //require(size == (nullCount + activeVertices.cardinality()))
    objectOutputStream.flush()
    ffiByteVectorOutput.finishSetting()
    val writenBytes = ffiByteVectorOutput.bytesWriten()
    log.info(s"write data array ${limit} of type ${GrapeUtils.getRuntimeClass[T].getName}, writen bytes ${writenBytes}")
    (ffiByteVectorOutput.getVector,ffiOffset)
  }
  def fillVertexTupleArrowArray[T : ClassTag](array: Array[T],activeVertices : ThreadSafeBitSet) : (FFIByteVector, FFIIntVector) = {
    val size = array.length
    val ffiByteVectorOutput = new FFIByteVectorOutputStream()
    //    val output = new Output(ffiByteVectorOutput)
    val ffiOffset = FFIIntVectorFactory.INSTANCE.create().asInstanceOf[FFIIntVector]
    ffiOffset.resize(size)
    ffiOffset.touch()
    val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
    var i = activeVertices.nextSetBit(0)
    val limit = size
    var prevBytesWritten = 0
    var nullCount = 0
    while (i < limit && i >= 0){
      if (array(i) == null){
        nullCount +=1
      }
      objectOutputStream.writeObject(array(i))
      ffiOffset.set(i, ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten)
      prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
      i += 1
    }
    log.info(s"total size ${size} null count ${nullCount}, active ${activeVertices.cardinality()}")
    //require(size == (nullCount + activeVertices.cardinality()))
    objectOutputStream.flush()
    ffiByteVectorOutput.finishSetting()
    val writenBytes = ffiByteVectorOutput.bytesWriten()
    log.info(s"write data array ${limit} of type ${GrapeUtils.getRuntimeClass[T].getName}, writen bytes ${writenBytes}")
    (ffiByteVectorOutput.getVector,ffiOffset)
  }

  def fillEdgeStringArrowArray[T : ClassTag](array: Array[T]) : (FFIByteVector, FFIIntVector) = {
    val size = array.length
    val ffiByteVectorOutput = new FFIByteVectorOutputStream()
    val ffiOffset = FFIIntVectorFactory.INSTANCE.create().asInstanceOf[FFIIntVector]
    ffiOffset.resize(size)
    ffiOffset.touch()
    val objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput)
    val limit = size
    var i = 0
    var prevBytesWritten = 0
    var nullCount = 0
    while (i < limit && i >= 0){
      if (array(i) == null){
        nullCount +=1
      }
      objectOutputStream.writeObject(array(i))
      ffiOffset.set(i, ffiByteVectorOutput.bytesWriten().toInt - prevBytesWritten)
      prevBytesWritten = ffiByteVectorOutput.bytesWriten().toInt
      i += 1
    }
    objectOutputStream.flush()
    ffiByteVectorOutput.finishSetting()
    val writenBytes = ffiByteVectorOutput.bytesWriten()
    log.info(s"write data array ${limit} of type ${GrapeUtils.getRuntimeClass[T].getName}, writen bytes ${writenBytes}")
    (ffiByteVectorOutput.getVector,ffiOffset)
  }


  def array2PrimitiveVertexData[T: ClassTag](array : Array[T], activeVertices : ThreadSafeBitSet, client : VineyardClient) : VertexData[Long,T] = {
    val builder = fillPrimitiveArrowArrayBuilder(array)
    val activeSetLongs = bitSet2longs(activeVertices)
    val newVdataBuilder = ScalaFFIFactory.newVertexDataBuilder[T]()
    newVdataBuilder.init(builder)
    newVdataBuilder.setBitsetWords(activeSetLongs.asInstanceOf[ArrowArrayBuilder[java.lang.Long]])
    newVdataBuilder.seal(client).get()
  }

  def array2PrimitiveEdgeData[T: ClassTag](array : Array[T], client : VineyardClient, numThread : Int) : EdgeData[Long,T] = {
//    val vector = fillPrimitiveVector(array, numThread)
//    val newEdataBuilder = ScalaFFIFactory.newEdgeDataBuilder[T](client,vector)
//    newEdataBuilder.seal(client).get()
    val newEdataBuilder = ScalaFFIFactory.newEdgeDataBuilder[T](client,array.size)
    fillPrimitiveVineyardArray(array,newEdataBuilder.getArrayBuilder,numThread)
    newEdataBuilder.seal(client).get()
  }

  def array2StringVertexData[T : ClassTag](array: Array[T],activeVertices : ThreadSafeBitSet,client: VineyardClient) : StringVertexData[Long,CXXStdString] = {
    val activeSetLongs = bitSet2longs(activeVertices)
    val (ffiByteVector,ffiIntVector) = if (getRuntimeClass[T].isInstance(Tuple2)){
      fillVertexTupleArrowArray(array,activeVertices)
    }
    else fillVertexStringArrowArray(array,activeVertices)
    val newVdataBuilder = ScalaFFIFactory.newStringVertexDataBuilder()
    newVdataBuilder.init(array.length, ffiByteVector, ffiIntVector)
    newVdataBuilder.setBitsetWords(activeSetLongs.asInstanceOf[ArrowArrayBuilder[java.lang.Long]])
    newVdataBuilder.seal(client).get()
  }

  def bitSet2longs(bitSetWithOffset: BitSetWithOffset) : ArrowArrayBuilder[Long] = {
    val longVector = ScalaFFIFactory.newSignedLongArrayBuilder()
    val words = bitSetWithOffset.bitset.words
    longVector.reserve(words.length)
    var i = 0
    while (i < words.length){
      longVector.unsafeAppend(words(i))
      i += 1
    }
    longVector
  }
  def bitSet2longs(bitSetWithOffset: ThreadSafeBitSet) : ArrowArrayBuilder[Long] = {
    val longVector = ScalaFFIFactory.newSignedLongArrayBuilder()
    val words = bitSetWithOffset.getWords
    longVector.reserve(words.length)
    var i = 0
    while (i < words.length){
      longVector.unsafeAppend(words(i))
      i += 1
    }
    longVector
  }

  def array2StringEdgeData[T : ClassTag](array: Array[T],client: VineyardClient) : StringEdgeData[Long,CXXStdString] = {
    val (ffiByteVector,ffiIntVector) = fillEdgeStringArrowArray(array)
    val newEdataBuilder = ScalaFFIFactory.newStringEdgeDataBuilder()
    newEdataBuilder.init(array.length, ffiByteVector, ffiIntVector)
    newEdataBuilder.seal(client).get()
  }

  def buildPrimitiveEdgeData[T : ClassTag](edgeStore : OffHeapEdgeDataStore[T], client : VineyardClient, localNum : Int): EdgeData[Long,T]  = {
    edgeStore.edataBuilder.seal(client).get()
  }
}
