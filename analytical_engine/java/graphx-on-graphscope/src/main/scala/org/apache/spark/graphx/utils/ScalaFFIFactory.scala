package org.apache.spark.graphx.utils

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.graphx.{BasicLocalVertexMapBuilder, LocalVertexMap, VineyardClient}
import org.apache.spark.internal.Logging

import java.util.HashMap
import scala.reflect.ClassTag

object ScalaFFIFactory extends Logging{
  private val arrowArrayBuilderMap = new HashMap[String, ArrowArrayBuilder.Factory[_]]
  def newLocalVertexMapBuilder(client : VineyardClient, innerOids : ArrowArrayBuilder[Long]): BasicLocalVertexMapBuilder[Long,Long] ={
     val localVertexMapBuilderFactory = FFITypeFactory.getFactory(classOf[BasicLocalVertexMapBuilder[Long,Long]], "gs::BasicLocalVertexMapBuilder<int64_t,uint64_t>").asInstanceOf[BasicLocalVertexMapBuilder.Factory[Long,Long]]
    localVertexMapBuilderFactory.create(client, innerOids)
  }

  def getArrowArrayBuilderFactory(foreignTypeName: String): ArrowArrayBuilder.Factory[_] = {
    if (!arrowArrayBuilderMap.containsKey(foreignTypeName)) {
      synchronized{
        if (!arrowArrayBuilderMap.containsKey(foreignTypeName)){
          arrowArrayBuilderMap.put(foreignTypeName, FFITypeFactory.getFactory(classOf[ArrowArrayBuilder[_]], foreignTypeName))
        }
      }
    }
    arrowArrayBuilderMap.get(foreignTypeName)
  }

  def newArrowArrayBuilder[T : ClassTag](clz: Class[T]): ArrowArrayBuilder[T] = {
    if (clz.equals(classOf[java.lang.Long]) || clz.equals(classOf[Long])){
      getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<int64_t>").create().asInstanceOf[ArrowArrayBuilder[T]]
    }
    else if (clz.equals(classOf[java.lang.Double]) || clz.equals(classOf[Double])){
      getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<double>").create().asInstanceOf[ArrowArrayBuilder[T]]
    }
    else if (clz.equals(classOf[Integer]) || clz.equals(classOf[Int])){
      getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<int32_t>").create().asInstanceOf[ArrowArrayBuilder[T]]
    }
    else throw new IllegalStateException("Not recognized " + clz.getName)
  }

  def newUnsignedLongArrayBuilder(): ArrowArrayBuilder[Long] ={
    getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<uint64_t>").create().asInstanceOf[ArrowArrayBuilder[Long]]
  }
  def newSignedLongArrayBuilder(): ArrowArrayBuilder[Long] ={
    getArrowArrayBuilderFactory("gs::ArrowArrayBuilder<int64_t>").create().asInstanceOf[ArrowArrayBuilder[Long]]
  }
}
