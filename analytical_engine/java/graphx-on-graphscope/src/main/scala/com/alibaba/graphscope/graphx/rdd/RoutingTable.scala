package com.alibaba.graphscope.graphx.rdd

import com.alibaba.graphscope.graphx.graph.GraphStructure
import com.alibaba.graphscope.graphx.utils.IdParser
import com.alibaba.graphscope.utils.ThreadSafeBitSet
import org.apache.spark.graphx.PartitionID
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable.ArrayBuffer

class RoutingTable(val pid2Lids : Array[ThreadSafeBitSet]) extends Serializable {
  val numPartitions = pid2Lids.length

  def get(ind : Int) : ThreadSafeBitSet = {
    pid2Lids(ind)
  }
}

object RoutingTable extends Logging{

  def fromGraphStructure(graphStructure: GraphStructure) : RoutingTable = {
    val res = new Array[ThreadSafeBitSet](graphStructure.fnum())
    val mirrorVertices = graphStructure.mirrorVertices
    for (fid <- 0 until graphStructure.fnum()){
      val pid = graphStructure.fid2GraphxPid(fid)
      res(pid) = mirrorVertices(fid)
    }
    new RoutingTable(res)
  }

//  type RoutingMessage = (PartitionID,Array[Long])
  /** to which partition these outer gids belongs. */
//  def generateMsg(fromPid : Int, numPartitions : Int, graphStructure: GraphStructure) : Iterator[(PartitionID, RoutingMessage)] = {
//    require(numPartitions == graphStructure.fnum())
//    val idParser = new IdParser(numPartitions)
//    val gids = new Array[ArrayBuffer[Long]](numPartitions)
//    for (pid <- 0 until numPartitions){
//      gids(pid) = new ArrayBuffer[Long]
//    }
//    val fid2GraphXPid = new Array[Int](numPartitions)
//    for (fid <- 0 until numPartitions){
//      fid2GraphXPid(fid) = graphStructure.fid2GraphxPid(fid)
//    }
//    log.info(s"fid to graphx pid mapping ${fid2GraphXPid.mkString("Array(", ", ", ")")}")
//    var lid = graphStructure.getInnerVertexSize
//    val limit = graphStructure.getVertexSize
//    while (lid < limit){
//      val gid = graphStructure.getOuterVertexGid(lid)
//      val fid = idParser.getFragId(gid)
//      val pid = fid2GraphXPid(fid)
//      gids(pid).+=(gid)
//      lid += 1
//    }
//    for (dst <- 0 until numPartitions){
//      log.info(s"Partition ${fromPid} send routing msg size ${gids(dst).size} to ${dst}")
//    }
//    gids.zipWithIndex.map(tuple => (tuple._2, (fromPid,tuple._1.toArray))).toIterator
//  }

//  def fromMsg(curPid : Int, numPartitions : Int, msgIter : Iterator[(PartitionID, RoutingMessage)], graphStructure: GraphStructure) : RoutingTable = {
//    val routingTable = new RoutingTable(numPartitions)
//    while (msgIter.hasNext){
//      val tuple = msgIter.next()
//      require(tuple._1 == curPid)
//      log.info(s"Partitions ${curPid} receives routing msg from ${tuple._2._1}, length ${tuple._2._2.length}")
//      routingTable.set(tuple._2._1, gidArrayToLidArray(graphStructure, curPid, tuple._2._2))
//    }
//    routingTable
//  }

//  def gidArrayToLidArray(graphStructure: GraphStructure, curPid : Int, longs: Array[Long]) : Array[Long] = {
//    val res = new Array[Long](longs.length)
//    var i = 0
//    val limit = longs.length
//    val idParser = new IdParser(graphStructure.fnum())
//    val fid2GraphXPid = new Array[Int](graphStructure.fnum())
//    for (fid <- 0 until graphStructure.fnum()){
//      fid2GraphXPid(fid) = graphStructure.fid2GraphxPid(fid)
//    }
//    while (i < limit){
//      val gid = longs(i)
//      val foundPid = fid2GraphXPid(idParser.getFragId(gid))
//      require(foundPid == curPid)
//      res(i) = idParser.getLocalId(gid)
//      i += 1
//    }
//    log.info(s"Partition ${curPid} finish processing gids to lids ${res.length}")
//    res
//  }
}
