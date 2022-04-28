package org.apache.spark.graphx.impl.graph

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import org.apache.spark.graphx.GraphXConf
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
//    values = Array.fill[VD](fragVerticesNum)(defaultOutVdata)
    values = new Array[VD](fragVerticesNum.intValue())
//    Array.fill[VD](fragVerticesNum)(defaultOutVdata)
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
}
