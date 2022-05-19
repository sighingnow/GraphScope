package org.apache.spark.graphx.utils

import com.alibaba.graphscope.graphx.{GraphXCSR, GraphXVertexMap, LocalVertexMap, VineyardClient}
import org.apache.spark.graphx.impl.EdgePartitionBuilder
import org.apache.spark.graphx.impl.partition.GrapeEdgePartitionBuilder

import scala.reflect.ClassTag

class GrapeMeta[VD: ClassTag, ED: ClassTag](val partitionID: Int, val partitionNum : Int, val vineyardClient : VineyardClient, val hostName : String) {

  var localVertexMap : LocalVertexMap[Long,Long] = null.asInstanceOf[LocalVertexMap[Long,Long]]
  var edgePartitionBuilder : GrapeEdgePartitionBuilder[VD,ED] = null.asInstanceOf[GrapeEdgePartitionBuilder[VD,ED]]
  var globalVMId : Long = -1
  var globalVM : GraphXVertexMap[Long,Long] = null.asInstanceOf[GraphXVertexMap[Long,Long]]
  var graphxCSR :GraphXCSR[Long,ED] = null.asInstanceOf[GraphXCSR[Long,ED]]
  def setLocalVertexMap(in : LocalVertexMap[Long,Long]): Unit ={
    this.localVertexMap = in
  }

  def setEdgePartitionBuilder(edgePartitionBuilder: GrapeEdgePartitionBuilder[VD,ED]) = {
    this.edgePartitionBuilder = edgePartitionBuilder
  }

  def setGlobalVM(globalVMId : Long) : Unit = {
    this.globalVMId = globalVMId
  }
  def setGlobalVM(globalVM : GraphXVertexMap[Long,Long]) : Unit = {
    this.globalVM = globalVM
  }
  def setCSR(csr : GraphXCSR[Long,ED]) : Unit = {
    this.graphxCSR = csr
  }
}
