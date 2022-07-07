package com.alibaba.graphscope.graphx.utils

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor
import com.alibaba.graphscope.fragment.{ArrowProjectedFragmentMapper, IFragment}
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.stdcxx.StdVector
import org.apache.spark.internal.Logging

import java.util.HashMap
import scala.reflect.ClassTag

object ScalaFFIFactory extends Logging{
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

  def newLongVector : StdVector[Long] = {
    val factory = FFITypeFactory.getFactory(classOf[StdVector[Long]],
      "std::vector<int64_t>").asInstanceOf[StdVector.Factory[Long]]
    factory.create()
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

  def newGraphXCSRBuilder(client : VineyardClient) : BasicGraphXCSRBuilder[Long,Long] = {
    val factory = FFITypeFactory.getFactory(classOf[BasicGraphXCSRBuilder[Long,Long]],
      "gs::BasicGraphXCSRBuilder<int64_t,uint64_t>").asInstanceOf[BasicGraphXCSRBuilder.Factory[Long,Long]]
    factory.create(client)
  }

  def newVertexDataBuilder[VD: ClassTag]() : VertexDataBuilder[Long,VD] = {
    val factory = FFITypeFactory.getFactory(classOf[VertexDataBuilder[Long,VD]],
      "gs::VertexDataBuilder<uint64_t," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[VD]) +">").asInstanceOf[VertexDataBuilder.Factory[Long,VD]]
    factory.create()
  }
  def newEdgeDataBuilder[VD: ClassTag]() : EdgeDataBuilder[Long,VD] = {
    val factory = FFITypeFactory.getFactory(classOf[EdgeDataBuilder[Long,VD]],
      "gs::EdgeDataBuilder<uint64_t," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[VD]) +">").asInstanceOf[EdgeDataBuilder.Factory[Long,VD]]
    factory.create()
  }

  def newStringVertexDataBuilder() : StringVertexDataBuilder[Long,String] = {
    val factory = FFITypeFactory.getFactory(classOf[StringVertexDataBuilder[Long,String]],
      "gs::VertexDataBuilder<uint64_t,std::string>").asInstanceOf[StringVertexDataBuilder.Factory[Long,String]]
    factory.create()
  }

  def newStringEdgeDataBuilder() : StringEdgeDataBuilder[Long,String] = {
    val factory = FFITypeFactory.getFactory(classOf[StringEdgeDataBuilder[Long,String]],
      "gs::EdgeDataBuilder<uint64_t,std::string>").asInstanceOf[StringEdgeDataBuilder.Factory[Long,String]]
    factory.create()
  }

  def newVineyardClient() : VineyardClient = {
    synchronized{
      clientFactory.create()
    }
  }

  def newGraphXFragmentBuilder[VD : ClassTag,ED : ClassTag](client : VineyardClient, vmId : Long, csrId : Long, vdId : Long, edId : Long) : GraphXFragmentBuilder[Long,Long,VD,ED] = {
    require(GrapeUtils.isPrimitive[VD] && GrapeUtils.isPrimitive[ED])
    val factory = FFITypeFactory.getFactory(classOf[GraphXFragmentBuilder[Long,Long,VD,ED]], "gs::GraphXFragmentBuilder<int64_t,uint64_t," +
        GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[VD]) + "," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[ED]) + ">").asInstanceOf[GraphXFragmentBuilder.Factory[Long,Long,VD,ED]]
    factory.create(client, vmId, csrId,vdId,edId)
  }

  def newGraphXStringVDFragmentBuiler[ED : ClassTag](client : VineyardClient, vmId : Long, csrId : Long, vdId : Long,edId : Long) : StringVDGraphXFragmentBuilder[Long,Long,String,ED] = {
    require(GrapeUtils.isPrimitive[ED])
    val factory = FFITypeFactory.getFactory(classOf[StringVDGraphXFragmentBuilder[Long,Long,String,ED]], "gs::GraphXFragmentBuilder<int64_t,uint64_t,std::string," +
        GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[ED]) + ">").asInstanceOf[StringVDGraphXFragmentBuilder.Factory[Long,Long,String,ED]]
    factory.create(client, vmId, csrId,vdId,edId)
  }
  def newGraphXStringEDFragmentBuilder[VD : ClassTag](client : VineyardClient, vmId : Long, csrId : Long, vdId : Long,edId : Long) : StringEDGraphXFragmentBuilder[Long,Long,VD,String] = {
    require(GrapeUtils.isPrimitive[VD])
    val factory = FFITypeFactory.getFactory(classOf[StringEDGraphXFragmentBuilder[Long,Long,VD,String]], "gs::GraphXFragmentBuilder<int64_t,uint64_t," +
        GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[VD]) + ",std::string>").asInstanceOf[StringEDGraphXFragmentBuilder.Factory[Long,Long,VD,String]]
    factory.create(client, vmId, csrId,vdId,edId)
  }

  def newGraphXStringVEDFragmentBuilder(client : VineyardClient, vmId : Long, csrId : Long, vdId : Long,edId : Long) : StringVEDGraphXFragmentBuilder[Long,Long,String,String] = {
    val factory = FFITypeFactory.getFactory(classOf[StringVEDGraphXFragmentBuilder[Long,Long,String,String]], "gs::GraphXFragmentBuilder<int64_t,uint64_t,std::string,std::string>").asInstanceOf[StringVEDGraphXFragmentBuilder.Factory[Long,Long,String,String]]
    factory.create(client, vmId, csrId,vdId,edId)
  }

  def newProjectedFragmentMapper[NEW_VD : ClassTag, NEW_ED : ClassTag, OLD_VD, OLD_ED](oldVdClz : Class[OLD_VD], oldEDClz : Class[OLD_ED]) : ArrowProjectedFragmentMapper[Long,Long,OLD_VD,NEW_VD,OLD_ED,NEW_ED] = {
    val factory = FFITypeFactory.getFactory(classOf[ArrowProjectedFragmentMapper[Long,Long,OLD_VD,NEW_VD,OLD_ED,NEW_ED]],
    "gs::ArrowProjectedFragmentMapper<int64_t,uint64_t," +GrapeUtils.classToStr(oldVdClz) + "," + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[NEW_VD]) + ","+ GrapeUtils.classToStr(oldEDClz) + ","
      + GrapeUtils.classToStr(GrapeUtils.getRuntimeClass[NEW_ED]) + ">").asInstanceOf[ArrowProjectedFragmentMapper.Factory[Long,Long,OLD_VD,NEW_VD,OLD_ED, NEW_ED]]
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
