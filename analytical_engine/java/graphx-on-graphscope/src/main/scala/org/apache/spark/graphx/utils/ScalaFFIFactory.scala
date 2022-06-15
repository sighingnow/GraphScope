package org.apache.spark.graphx.utils

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor
import com.alibaba.graphscope.fragment.{ArrowProjectedFragment, IFragment}
import com.alibaba.graphscope.graphx.{ArrowProjectedFragmentGetter, BasicGraphXCSRBuilder, BasicLocalVertexMapBuilder, GraphXCSRMapper, GraphXVertexMapGetter, LocalVertexMap, StringVertexDataBuilder, VertexDataBuilder, VineyardClient}
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.internal.Logging

import java.util.HashMap
import scala.reflect.ClassTag

object ScalaFFIFactory extends Logging{
  System.loadLibrary("grape-jni")
  private val arrowArrayBuilderMap = new HashMap[String, ArrowArrayBuilder.Factory[_]]
  val clientFactory : VineyardClient.Factory = FFITypeFactory.getFactory(classOf[VineyardClient],"vineyard::Client").asInstanceOf[VineyardClient.Factory]
  def newLocalVertexMapBuilder(client : VineyardClient, innerOids : ArrowArrayBuilder[Long],
                               outerOids : ArrowArrayBuilder[Long]): BasicLocalVertexMapBuilder[Long,Long] ={
     val localVertexMapBuilderFactory = FFITypeFactory.getFactory(classOf[BasicLocalVertexMapBuilder[Long,Long]], "gs::BasicLocalVertexMapBuilder<int64_t,uint64_t>").asInstanceOf[BasicLocalVertexMapBuilder.Factory[Long,Long]]
    localVertexMapBuilderFactory.create(client, innerOids, outerOids)
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

  def newVertexMapGetter() : GraphXVertexMapGetter[Long,Long] = {
    val factory = FFITypeFactory.getFactory(classOf[GraphXVertexMapGetter[Long,Long]], "gs::GraphXVertexMapGetter<int64_t,uint64_t>").asInstanceOf[GraphXVertexMapGetter.Factory[Long,Long]]
    factory.create()
  }

  def newGraphXCSRBuilder[ED: ClassTag](client : VineyardClient) : BasicGraphXCSRBuilder[Long,Long,ED] = {
    val factory = FFITypeFactory.getFactory(classOf[BasicGraphXCSRBuilder[Long,Long,ED]],
      "gs::BasicGraphXCSRBuilder<int64_t,uint64_t," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[ED]) +">").asInstanceOf[BasicGraphXCSRBuilder.Factory[Long,Long,ED]]
    factory.create(client)
  }

  def newVertexDataBuilder[VD: ClassTag]() : VertexDataBuilder[Long,VD] = {
    val factory = FFITypeFactory.getFactory(classOf[VertexDataBuilder[Long,VD]],
      "gs::VertexDataBuilder<uint64_t," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[VD]) +">").asInstanceOf[VertexDataBuilder.Factory[Long,VD]]
    factory.create()
  }

  def newStringVertexDataBuilder() : StringVertexDataBuilder = {
    val factory = FFITypeFactory.getFactory(classOf[StringVertexDataBuilder],
      "gs::VertexDataBuilder<uint64_t,std::string>").asInstanceOf[StringVertexDataBuilder.Factory]
    factory.create()
  }

  def newVineyardClient() : VineyardClient = {
    synchronized{
      clientFactory.create()
    }
  }

  def newGraphXCSRMapper[NEW_ED : ClassTag](oldEDClz : Class[_]) : GraphXCSRMapper[Long,_,NEW_ED] = {
    val factory = FFITypeFactory.getFactory(classOf[GraphXCSRMapper[Long,_,NEW_ED]],
      "gs::GraphXCSRMapper<uint64_t," + GrapeUtils.classToStr(oldEDClz) + ","
        + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[NEW_ED]) + ">").asInstanceOf[GraphXCSRMapper.Factory[Long,_,NEW_ED]]
    factory.create()
  }

  def getFragment[VD : ClassTag,ED : ClassTag](client : VineyardClient, objectID : Long, fragStr : String) : IFragment[Long,Long,VD,ED]= {
    if (fragStr.startsWith("gs::ArrowFragment")){
      throw new IllegalStateException("Not implemented now")
    }
    else if (fragStr.startsWith("gs::ArrowProjectedFragment")){
      log.info(s"Getting fragment for ${fragStr}, ${objectID}")
      val getterStr = fragName2FragGetterName(fragStr)
      val factory = FFITypeFactory.getFactory(classOf[ArrowProjectedFragmentGetter[Long,Long,VD,ED]], getterStr).asInstanceOf[ArrowProjectedFragmentGetter.Factory[Long,Long,VD,ED]]
      val fragmentGetter = factory.create()
      val res = fragmentGetter.get(client, objectID)
      new ArrowProjectedAdaptor[Long,Long,VD,ED](res.get())
    }
    else {
      throw new IllegalStateException(s"Not recognized frag str ${fragStr}")
    }
  }
  /** transform the frag name to frag getter name. */
  def fragName2FragGetterName(str : String) : String = {
    if (str.contains("ArrowProjectedFragment")){
      str.replace("ArrowProjectedFragment", "ArrowProjectedFragmentGetter")
    }
    else if (str.contains("ArrowFragment")){
      str.replace("ArrowFragment", "ArrowFragmentGetter")
    }
    else {
      throw new IllegalStateException(s"Not recognized ${str}")
    }
  }


}
