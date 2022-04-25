package org.apache.spark.graphx.impl

import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GrapeVertexPartition[VD : ClassTag](pid : Int, grapeVertexMapPartition: GrapeVertexMapPartition, defaultValue : VD) extends Logging{
  val innerVertexNum: Int = grapeVertexMapPartition.innerVertexNum
  val outerVertexNum: Int = grapeVertexMapPartition.outerVertexNum
  val totalVertexNum : Int = grapeVertexMapPartition.totalVertexNum

  val data = Array.fill(totalVertexNum)(defaultValue)

  val innerVertices : Range = 0 until innerVertexNum
  val outerVertices : Range = innerVertexNum until totalVertexNum


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
}
