package org.apache.spark.graphx.impl.graph

import com.alibaba.graphscope.ds.{Vertex}
import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.utils.{FFITypeFactoryhelper, MappedBuffer}
import org.apache.spark.graphx.GraphXConf
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.traits.VertexDataManager
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class VertexDataManagerImpl[VD : ClassTag,ED : ClassTag](
        val conf : GraphXConf[VD,ED], var values : Array[VD] = null, private val fragment: IFragment[Long,Long,_,_],
        val buffer : MappedBuffer,
        val defaultOutVdata : VD = null.asInstanceOf[VD]) extends VertexDataManager[VD] with Logging{

  val innerVerticesNum: Long = fragment.getInnerVerticesNum
  val fragVerticesNum : Long = fragment.getVerticesNum
  if (values == null){
    initValues
  }
  private def initValues  = {
    values = new Array[VD](fragVerticesNum.intValue())
    if (buffer == null) {
      log.info(s"[Frag${fragment.fid()}]Init vertex data with default out vdata ${defaultOutVdata}, iv data are from fragment]")
      val vertex: Vertex[Long] = FFITypeFactoryhelper.newVertexLong.asInstanceOf[Vertex[Long]]

      var lid = 0
      while (lid < innerVerticesNum) {
        vertex.SetValue(lid)
        values(lid) = fragment.getData(vertex).asInstanceOf[VD]
        lid += 1
      }
      while (lid < fragVerticesNum) {
        values(lid) = defaultOutVdata
        lid += 1
      }
      //FIXME: ArrowProjectedFragment stores not outer vertex data
    }
    else {
      log.info(s"load vdata from shared memory ${buffer}")
      var offset = 0
      val totalLength = buffer.readLong(offset)
      log.info(s"from buffer: length of vdata memory ${totalLength}")
      require(totalLength == innerVerticesNum, s"inv num and length should mathc ${totalLength}, ${innerVerticesNum}")
      val vdBytes = GrapeUtils.bytesForType[VD](conf.getVdClass)
      val limit = innerVerticesNum * vdBytes + 8
      log.info(s"bytes limit ${limit}")
      offset += 8
      var lid = 0
      if (conf.getVdClass.equals(classOf[Long]) || conf.getVdClass.equals(classOf[java.lang.Long])){
        while (offset < limit && lid < values.length){
          values(lid) = buffer.readLong(offset).asInstanceOf[VD]
          lid += 1;
          offset += 8
        }
      }
      else if (conf.getVdClass.equals(classOf[Double]) || conf.getVdClass.equals(classOf[java.lang.Double])){
        while (offset < limit && lid < values.length){
          values(lid) = buffer.readDouble(offset).asInstanceOf[VD]
          lid += 1;
          offset += 8
        }
      }
      else if (conf.getVdClass.equals(classOf[Int]) ||conf.getVdClass.equals(classOf[Integer])){
        while (offset < limit && lid < values.length){
          values(lid) = buffer.readInt(offset).asInstanceOf[VD]
          lid += 1;
          offset += 4
        }
      }
      else {
        throw new IllegalStateException("not recognized vd class")
      }
      require(offset == limit && lid == values.length, s"after read vdata, size not mathc ${offset} vs ${limit}, ${lid} vs ${values.length}")
    }
    log.info("Create Vertex Data Manager: " + fragment.getVerticesNum)
    printvdatas()
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
    new VertexDataManagerImpl[VD2,ED](new GraphXConf[VD2,ED], newVertexData, fragment, buffer, null.asInstanceOf[VD2])
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

  def printvdatas() : Unit = {
    var ind = 0
    while (ind < innerVerticesNum.toInt){
      println(s"lid: ${ind}, vdata ${values(ind)}")
      ind += 1
    }
  }
}
