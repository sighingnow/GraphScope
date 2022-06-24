package org.apache.spark.graphx.utils

import com.alibaba.graphscope.graphx.{GraphXCSR, GraphXVertexMap, LocalVertexMap, VineyardClient}
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.impl.grape.GrapeEdgePartitionBuilder
import org.apache.spark.graphx.impl.partition.RoutingTable
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GrapeMeta[VD: ClassTag, ED: ClassTag](val partitionID: Int, val partitionNum : Int, val vineyardClient : VineyardClient, val hostName : String) extends Logging {

  var localVertexMap : LocalVertexMap[Long,Long] = null.asInstanceOf[LocalVertexMap[Long,Long]]
  var edgePartitionBuilder : GrapeEdgePartitionBuilder[VD,ED] = null.asInstanceOf[GrapeEdgePartitionBuilder[VD,ED]]
  var globalVMId : Long = -1
  var globalVM : GraphXVertexMap[Long,Long] = null.asInstanceOf[GraphXVertexMap[Long,Long]]
  var graphxCSR :GraphXCSR[Long] = null.asInstanceOf[GraphXCSR[Long]]
  var routingTable : RoutingTable = null.asInstanceOf[RoutingTable]
  var edataArray : PrimitiveArray[ED] = null.asInstanceOf[PrimitiveArray[ED]]
  def setLocalVertexMap(in : LocalVertexMap[Long,Long]): Unit ={
    this.localVertexMap = in
  }

  def setEdgePartitionBuilder(edgePartitionBuilder: GrapeEdgePartitionBuilder[VD,ED]) = {
    this.edgePartitionBuilder = edgePartitionBuilder
  }

  def setEdataArray(ed : PrimitiveArray[ED]) : Unit = {
    this.edataArray = ed
  }

  def setGlobalVM(globalVMId : Long) : Unit = {
    log.info(s"setting global vm id ${globalVMId}")
    this.globalVMId = globalVMId
  }
  def setGlobalVM(globalVM : GraphXVertexMap[Long,Long]) : Unit = {
    this.globalVM = globalVM
  }
  def setCSR(csr : GraphXCSR[Long]) : Unit = {
    this.graphxCSR = csr
  }

  def setRoutingTable(routingTable: RoutingTable) : Unit = {
    this.routingTable = routingTable
  }
}
