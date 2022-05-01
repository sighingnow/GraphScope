package org.apache.spark.graphx.impl.graph

import com.alibaba.graphscope.ds.{TypedArray, Vertex}
import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor
import com.alibaba.graphscope.utils.{FFITypeFactoryhelper, MappedBuffer}
import org.apache.spark.graphx.GraphXConf
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.traits.VertexDataManager
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class VertexDataManagerImpl[VD : ClassTag,ED : ClassTag](
        val conf : GraphXConf[VD,ED], var values : Array[VD] = null, private val fragment: IFragment[Long,Long,_,_],
        val defaultOutVdata : VD = null.asInstanceOf[VD]) extends VertexDataManager[VD] with Logging{

  val innerVerticesNum: Long = fragment.getInnerVerticesNum
  val fragVerticesNum : Long = fragment.getVerticesNum
  if (values == null){
    initValues
  }
  private def initValues  = {
    values = new Array[VD](fragVerticesNum.intValue())
    val vertex: Vertex[Long] = FFITypeFactoryhelper.newVertexLong.asInstanceOf[Vertex[Long]]

    var lid = 0
    while (lid < innerVerticesNum){
      vertex.SetValue(lid)
      values(lid) = fragment.getData(vertex).asInstanceOf[VD]
      lid += 1
    }
    while (lid < fragVerticesNum){
      values(lid) = defaultOutVdata
      lid += 1
    }
    //FIXME: ArrowProjectedFragment stores not outer vertex data
    log.info("Create Vertex Data Manager: " + fragment.getVerticesNum)
  }
  override def setValues(vdatas: Array[VD]): Unit = {
    this.values = vdatas
  }

  override def getVertexData(lid: Long): VD = {
    values(lid.toInt)
  }

  override def setVertexData(lid: Long, vertexData: VD): Unit = {
    values(lid.toInt)= vertexData
  }

  override def withNewVertexData[VD2: ClassTag](newVertexData: Array[VD2]): VertexDataManager[VD2] = {
    new VertexDataManagerImpl[VD2,ED](new GraphXConf[VD2,ED], newVertexData, fragment, null.asInstanceOf[VD2])
  }

  override def writeBackVertexData(buffer: MappedBuffer): Unit = {
    require(buffer.remaining() > 8 * innerVerticesNum, s"not enough space ${buffer.remaining()}, at least : ${8 * innerVerticesNum}")
    var lid = 0;
    val vdClass = GrapeUtils.getRuntimeClass[VD].asInstanceOf[Class[VD]]
    buffer.writeLong(innerVerticesNum)
    if (vdClass.equals(classOf[Long])){
      while (lid < innerVerticesNum){
        buffer.writeLong(values(lid).asInstanceOf[Long])
        lid += 1
      }
    }
    else if (vdClass.equals(classOf[Double])){
      while (lid < innerVerticesNum){
        buffer.writeDouble(values(lid).asInstanceOf[Double])
        lid += 1
      }
    }
    else if (vdClass.equals(classOf[Integer])){
      while (lid < innerVerticesNum){
        buffer.writeInt(values(lid).asInstanceOf[Integer])
        lid += 1
      }
    }
    else {
      throw new IllegalStateException("not recognized vd class")
    }
    log.info(s"Finish writing bytes to mapped buffer for vdata")
  }
}
