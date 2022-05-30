package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.graphx.GraphXVertexMap
import org.apache.spark.graphx.PartitionID
import org.apache.spark.internal.Logging

import scala.collection.mutable.ArrayBuffer

class RoutingTable(val numPartitions : Int) {
  private val pid2Lids = new Array[Array[Long]](numPartitions)

  def set(ind : Int, gidArray : Array[Long]) : Unit = {
    require(pid2Lids(ind) == null, s"try to insert to ${ind}, but no empty")
    pid2Lids(ind) = gidArray
  }

  def get(ind : Int) : Array[Long] = {
    pid2Lids(ind)
  }

}

object RoutingTable extends Logging{
  type RoutingMessage = (PartitionID,Array[Long])
  /** to which partition these outer gids belongs. */
  def generateMsg(fromPid : Int, numPartitions : Int, vm : GraphXVertexMap[Long,Long]) : Iterator[(PartitionID, RoutingMessage)] = {
    require(numPartitions == vm.fnum())
    val idParser = new IdParser(numPartitions)
    val gids = new Array[ArrayBuffer[Long]](numPartitions)
    for (pid <- 0 until numPartitions){
      gids(pid) = new ArrayBuffer[Long]
    }
    val fid2GraphXPid = new Array[Int](numPartitions)
    for (fid <- 0 until numPartitions){
      fid2GraphXPid(fid) = vm.fid2GraphxPid(fid)
    }
    log.info(s"fid to graphx pid mapping ${fid2GraphXPid.mkString("Array(", ", ", ")")}")
    var lid = vm.innerVertexSize()
    val limit = vm.getVertexSize
    while (lid < limit){
      val gid = vm.getOuterVertexGid(lid)
      val fid = idParser.getFragId(gid)
      val pid = fid2GraphXPid(fid)
      gids(pid).+=(gid)
      lid += 1
    }
    for (dst <- 0 until numPartitions){
      log.info(s"Partition ${fromPid} send routing msg size ${gids(dst).size} to ${dst}")
    }
    gids.zipWithIndex.map(tuple => (tuple._2, (fromPid,tuple._1.toArray))).toIterator
  }

  def fromMsg(curPid : Int, numPartitions : Int, msgIter : Iterator[(PartitionID, RoutingMessage)], vm : GraphXVertexMap[Long,Long]) : RoutingTable = {
    val routingTable = new RoutingTable(numPartitions)
    while (msgIter.hasNext){
      val tuple = msgIter.next()
      require(tuple._1 == curPid)
      log.info(s"Partitions ${curPid} receives routing msg from ${tuple._2._1}, length ${tuple._2._2.length}")
      routingTable.set(tuple._2._1, gidArrayToLidArray(vm, curPid, tuple._2._2))
    }
    routingTable
  }

  def gidArrayToLidArray(vm : GraphXVertexMap[Long,Long], curPid : Int, longs: Array[Long]) : Array[Long] = {
    val res = new Array[Long](longs.length)
    var i = 0
    val limit = longs.length
    val idParser = new IdParser(vm.fnum())
    val fid2GraphXPid = new Array[Int](vm.fnum())
    for (fid <- 0 until vm.fnum()){
      fid2GraphXPid(fid) = vm.fid2GraphxPid(fid)
    }
    while (i < limit){
      val gid = longs(i)
      val foundPid = fid2GraphXPid(idParser.getFragId(gid))
      require(foundPid == curPid)
      res(i) = idParser.getLocalId(gid)
      i += 1
    }
    log.info(s"Partition ${curPid} finish processing gids to lids ${res.length}")
    res
  }
}
