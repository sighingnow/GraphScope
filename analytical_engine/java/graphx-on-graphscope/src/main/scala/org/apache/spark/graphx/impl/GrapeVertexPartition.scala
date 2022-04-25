package org.apache.spark.graphx.impl

import org.apache.spark.graphx.VertexId
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GrapeVertexPartition[VD : ClassTag](pid : Int, grapeVertexMapPartition: GrapeVertexMapPartition, defaultValue : VD, data : Array[VD]) extends Logging{
  val innerVertexNum: Int = grapeVertexMapPartition.innerVertexNum
  val outerVertexNum: Int = grapeVertexMapPartition.outerVertexNum
  val totalVertexNum : Int = grapeVertexMapPartition.totalVertexNum


  val innerVertices : Range = 0 until innerVertexNum
  val outerVertices : Range = innerVertexNum until totalVertexNum

  def iterator() : Iterator[(VertexId, VD)] = {
    new Iterator[(VertexId,VD)] {
      private[this] var lid = 0

      override def hasNext: Boolean = lid < innerVertexNum

      override def next(): (VertexId,VD) = {
        val res = (grapeVertexMapPartition.innerVertexLid2Oid(lid),data(lid))
        lid += 1
        res
      }
    }
  }

  def ivLid2Oid: Array[Long] = grapeVertexMapPartition.getIvLid2Oid

  def vdataArray : Array[VD] = data

  def updateData(oid : Long, newData : VD): Unit ={
    val lid = grapeVertexMapPartition.ivOid2Lid(oid).toInt
    if (lid == -1){
      log.info(s"Partition: ${pid} got vertex ${oid}, outer vertex ${lid}, not update");
    }
    else {
      if (lid >= innerVertexNum){
        log.error(s"Internal error: got inner vertex lid  ${lid} greater than ivnum ${innerVertexNum}")
      }
      log.info(s"Partition: ${pid} got vertex ${oid}, inner vertex ${lid}, update to ${newData}");
      data.update(lid, newData)
    }
  }

  def map[VD2 : ClassTag](f: (VertexId, VD) => VD2) : GrapeVertexPartition[VD2] = {
    val newValues = new Array[VD2](innerVertexNum)
    var i = 0
    while (i < innerVertexNum) {
      newValues(i) = f(ivLid2Oid(i), data(i))
      i += 1
    }
    new GrapeVertexPartition[VD2](pid, grapeVertexMapPartition,null.asInstanceOf[VD2], newValues)
  }
}
