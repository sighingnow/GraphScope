package org.apache.spark.graphx.impl

import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

import scala.reflect.ClassTag

class GrapeVertexMapPartition(ivLid2Oid : Array[Long], ovLid2Oid : Array[Long],
                              ivOid2Lid : GraphXPrimitiveKeyOpenHashMap[VertexId,Long], ovOid2Lid : GraphXPrimitiveKeyOpenHashMap[VertexId,Long],
                              ovOid2Fid :  GraphXPrimitiveKeyOpenHashMap[VertexId,Int]) {

  val innerVertexNum : Int = ivLid2Oid.length
  val outerVertexNum: Int = ovLid2Oid.length
  val totalVertexNum : Int = innerVertexNum + outerVertexNum

  def getIvLid2Oid = ivLid2Oid

  def toVertexPartition[VD : ClassTag](defaultValue : VD) : GrapeVertexPartition[VD] = {
    new GrapeVertexPartition[VD](this, defaultValue)
  }

}
