package org.apache.spark.graphx.utils

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.graphx.GrapeEdgePartition
import com.alibaba.graphscope.utils.ReflectUtils
import org.apache.spark.graphx.impl.{GrapeEdgePartitionWrapper, GrapeUtils}
import org.apache.spark.internal.Logging

import java.lang.reflect.Field
import java.util
import java.util.Vector
import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

class GrapeEdgePartitionRegistry[VD: ClassTag, ED: ClassTag] extends Logging{
  private val partitionNum : AtomicInteger = new AtomicInteger(0)
  private val partitionCnt : AtomicInteger = new AtomicInteger(0)
  val edClass = GrapeUtils.getRuntimeClass[ED].asInstanceOf[Class[ED]]
  private var grapeEdgePartition : GrapeEdgePartition[Long,Long,ED]  = null.asInstanceOf[GrapeEdgePartition[Long,Long,ED]]
  private var srcOidBuilder : ArrowArrayBuilder[Long] = null.asInstanceOf[ArrowArrayBuilder[Long]]
  private var dstOidBuilder : ArrowArrayBuilder[Long] = null.asInstanceOf[ArrowArrayBuilder[Long]]
  private var edataBuilder : ArrowArrayBuilder[ED] = null.asInstanceOf[ArrowArrayBuilder[ED]]


  log.info(s" builder cl: ${classOf[ArrowArrayBuilder[_]].getClassLoader.toString}")
  log.info(s" context class loader: ${Thread.currentThread().getContextClassLoader.toString}")

  def createArrayBuilder(pid : Int) : Unit = {
    partitionNum.addAndGet(1)
    if (srcOidBuilder == null){
      synchronized{
        if (srcOidBuilder == null){
	        FFITypeFactory.loadClassLoader(Thread.currentThread().getContextClassLoader)
          val libs = ClassScope.getLoadedLibraries(GrapeEdgePartitionRegistry.getClass.getClassLoader)
          log.info(s"${libs.mkString("Array(", ", ", ")")}")
          val factory = FFITypeFactory.getFactory(classOf[ArrowArrayBuilder[_]], "gs::ArrowArrayBuilder<int64_t>").asInstanceOf[ArrowArrayBuilder.Factory[Long]]
 	  log.info(s"ffi type cl: ${factory.getClass.getClassLoader}")
          srcOidBuilder = factory.create()
          dstOidBuilder = factory.create()
          log.info(s"Partition ${pid} create oid builders ${srcOidBuilder} ${dstOidBuilder}")
          val edataFactory = FFITypeFactory.getFactory(classOf[ArrowArrayBuilder[_]], "gs::ArrowArrayBuilder<" + GrapeUtils.classToStr(edClass) +">").asInstanceOf[ArrowArrayBuilder.Factory[ED]]
          edataBuilder = edataFactory.create()
          log.info(s"Partitoin ${pid} create edata builder ${edataBuilder}")
          return
        }
      }
    }
    log.info(s"Partition ${pid} already has builder ${srcOidBuilder}")
  }

  def getBuilders() : (ArrowArrayBuilder[Long],ArrowArrayBuilder[Long],ArrowArrayBuilder[ED]) = {
    (srcOidBuilder,dstOidBuilder,edataBuilder)
  }

  def constructEdgePartition(pid : Int) : Unit = {
    if (grapeEdgePartition == null){
      synchronized{
        if (grapeEdgePartition == null){
          log.info(s"Partition [${pid}] try to construct grape partition")
          grapeEdgePartition = ReflectUtils.invokeEdgePartitionCreation(
            classOf[java.lang.Long].asInstanceOf[Class[_ <: Long]],
            classOf[java.lang.Long].asInstanceOf[Class[_ <: Long]],
            GrapeUtils.getRuntimeClass[ED].asInstanceOf[Class[_ <: ED]])
	          log.info(s"Partition [${pid}] finish constructing edge partition ${grapeEdgePartition.toString}")
          grapeEdgePartition.loadEdges(srcOidBuilder,dstOidBuilder,edataBuilder)
          log.info(s"Partition [${pid}] finish loading edges, numEdges ${grapeEdgePartition.getEdgesNum} num vertices: ${grapeEdgePartition.getVerticesNum}")
          return
        }
      }
    }
    log.info(s"Partition [${pid}] skip construct grape partition")
  }

  /** We can not use pid as index, since on one executor the partiton num may be not necessarily consecutive */
  def getEdgePartitionWrapper(pid : Int): GrapeEdgePartitionWrapper[VD,ED] ={
    synchronized{
      val curPartId = partitionCnt.getAndAdd(1);
      val numParts = partitionNum.get()
      val totalVertices = grapeEdgePartition.getVerticesNum()
      log.info(s"cur ${curPartId}, num parts ${numParts}, total vnum ${totalVertices}")
      val chunkSize = (totalVertices + numParts - 1) / numParts
      log.info(s"chunk size ${chunkSize}")
      val startLid = chunkSize * curPartId
      val endLid = Math.min(startLid + chunkSize, grapeEdgePartition.getVerticesNum)
      log.info(s"cur pid ${curPartId} start from ${startLid} to ${endLid}")
      new GrapeEdgePartitionWrapper[VD,ED](pid, startLid, endLid, grapeEdgePartition)
    }
  }
}

object GrapeEdgePartitionRegistry extends Logging{
  System.loadLibrary("grape-jni")
  log.info("[NativeUtils:] load jni lib success")
  private var registry = null.asInstanceOf[GrapeEdgePartitionRegistry[_,_]]
  def getOrCreate[VD: ClassTag,ED : ClassTag] : GrapeEdgePartitionRegistry[VD,ED] = {
    if (registry == null){
      synchronized{
        if (registry == null){
          registry = new GrapeEdgePartitionRegistry[VD,ED]
        }
      }
    }
    registry.asInstanceOf[GrapeEdgePartitionRegistry[VD,ED]]
  }

  /**
   * Clear for the next time usages.
   */
  def clear(): Unit ={
    registry = null
  }
}
object ClassScope {
  private var LIBRARIES: Field = null
  try LIBRARIES = classOf[ClassLoader].getDeclaredField("loadedLibraryNames")
  catch {
    case e: NoSuchFieldException =>
      e.printStackTrace()
  }
  LIBRARIES.setAccessible(true)

  @throws[IllegalAccessException]
  def getLoadedLibraries(loader: ClassLoader): Array[String] = {
    val libraries: java.util.Vector[String] = LIBRARIES.get(loader).asInstanceOf[java.util.Vector[String]]
    libraries.toArray(new Array[String](libraries.size()))
  }
}
