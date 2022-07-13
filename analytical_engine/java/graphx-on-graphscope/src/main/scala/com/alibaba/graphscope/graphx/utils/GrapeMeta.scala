package com.alibaba.graphscope.graphx.utils

import com.alibaba.graphscope.graphx.rdd.RoutingTable
import com.alibaba.graphscope.graphx.rdd.impl.GrapeEdgePartitionBuilder
import com.alibaba.graphscope.graphx.{GraphXCSR, GraphXVertexMap, LocalVertexMap, VineyardClient}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GrapeMeta[VD: ClassTag, ED: ClassTag](val partitionID: Int, val partitionNum : Int, val vineyardClient : VineyardClient, val hostName : String) extends Logging {

  var localVertexMap : LocalVertexMap[Long,Long] = null.asInstanceOf[LocalVertexMap[Long,Long]]
  var edgePartitionBuilder : GrapeEdgePartitionBuilder[VD,ED] = null.asInstanceOf[GrapeEdgePartitionBuilder[VD,ED]]
  var globalVMId : Long = -1
  var globalVM : GraphXVertexMap[Long,Long] = null.asInstanceOf[GraphXVertexMap[Long,Long]]
  var graphxCSR :GraphXCSR[Long] = null.asInstanceOf[GraphXCSR[Long]]
  var routingTable : RoutingTable = null.asInstanceOf[RoutingTable]
  var edataArray : Array[ED] = null.asInstanceOf[Array[ED]]
  var eids : Array[Long] = null.asInstanceOf[Array[Long]]
  def setLocalVertexMap(in : LocalVertexMap[Long,Long]): Unit ={
    this.localVertexMap = in
  }

  def setEdgePartitionBuilder(edgePartitionBuilder: GrapeEdgePartitionBuilder[VD,ED]) = {
    this.edgePartitionBuilder = edgePartitionBuilder
  }

  def setEdataArray(ed : Array[ED]) : Unit = {
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

  def setEids(eids : Array[Long]) : Unit = {
    this.eids = eids
  }
  def setRoutingTable(routingTable: RoutingTable) : Unit = {
    this.routingTable = routingTable
  }
}
