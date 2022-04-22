package org.apache.spark.graphx

import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

/**
 * A vertex map should contains global oid->gid mapping. But here we just keep our current partition
 */
class VertexMapRDD(ivLid2Oid : Array[Long], ovLid2Oid : Array[Long],
                   ivOid2Lid : GraphXPrimitiveKeyOpenHashMap[VertexId,Long],
                   ovOid2Lid : GraphXPrimitiveKeyOpenHashMap[VertexId,Long],
                   ovOid2Fid :  GraphXPrimitiveKeyOpenHashMap[VertexId,Int]) extends Serializable {

  def getInnerVertexOid(index : Int) = ivLid2Oid(index)
  def getOuterVertexOid(index : Int) = ovLid2Oid(index)

}
