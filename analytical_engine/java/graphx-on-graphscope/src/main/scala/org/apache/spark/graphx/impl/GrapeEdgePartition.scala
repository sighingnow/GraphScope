package org.apache.spark.graphx.impl

import org.apache.spark.Partitioner
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Contains the edges after shuffle. we should be able to engage in fragment building with no more
 * shuffle, from this edgepartition
 */
class GrapeEdgePartition[@specialized(Char, Long, Int, Double) ED: ClassTag]
(srcOids: Array[Long], dstOids: Array[Long], edatas: Array[ED], val ivEdgeNum : Long, val ovEdgeNum: Long, vertexMapPartition: GrapeVertexMapPartition) extends Serializable {
  /**
   * edges with src id belongs to our partition;
   * edges with dstId belongs to our partition.
   */
  val totalEdgeNum : Long = ivEdgeNum + ovEdgeNum
  require(totalEdgeNum == ivEdgeNum + ovEdgeNum, s"Inconsistent ${totalEdgeNum}, ${ivEdgeNum}, ${ovEdgeNum}")
  require(totalEdgeNum == srcOids.length, s"Inconsistent ${totalEdgeNum},${srcOids.length}")




  def iterator() : Iterator[Edge[ED]] = {
    new Iterator[Edge[ED]] {
      private[this] val edge = new Edge[ED]
      private[this] var lid = 0

      override def hasNext: Boolean = lid < totalEdgeNum

      override def next(): Edge[ED] = {
        edge.srcId = srcOid(lid)
        edge.dstId = dstOid(lid)
        edge.attr = edgeData(lid)
        lid += 1
        edge
      }
    }
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[ED2] = {
    val newData = new Array[ED2](edatas.length)
    val edge = new Edge[ED]()
    val size = edatas.length
    var i = 0
    while (i < size) {
      edge.srcId = srcOid(i)
      edge.dstId = dstOids(i)
      edge.attr = edatas(i)
      newData(i) = f(edge)
      i += 1
    }
    this.withData(newData)
  }

//  def innerJoin[ED2: ClassTag, ED3: ClassTag]
//  (other: GrapeEdgePartition[ED2])
//  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgePartition[ED3] = {
//    val builder = new ExistingGrapeEdgePartitionBuilder[ED3](
//      global2local, local2global, vertexAttrs, activeSet)
//    var i = 0
//    var j = 0
//    // For i = index of each edge in `this`...
//    while (i < size && j < other.size) {
//      val srcId = this.srcIds(i)
//      val dstId = this.dstIds(i)
//      // ... forward j to the index of the corresponding edge in `other`, and...
//      while (j < other.size && other.srcIds(j) < srcId) { j += 1 }
//      if (j < other.size && other.srcIds(j) == srcId) {
//        while (j < other.size && other.srcIds(j) == srcId && other.dstIds(j) < dstId) { j += 1 }
//        if (j < other.size && other.srcIds(j) == srcId && other.dstIds(j) == dstId) {
//          // ... run `f` on the matching edge
//          builder.add(srcId, dstId, localSrcIds(i), localDstIds(i),
//            f(srcId, dstId, this.data(i), other.attrs(j)))
//        }
//      }
//      i += 1
//    }
//    builder.toEdgePartition
//  }

  /**
   * Create new GrapEdgePartition by only updating the edge data.
   * @param newData new data
   * @tparam ED2 new edata type
   * @return
   */
  def withData[ED2: ClassTag](newData: Array[ED2]): GrapeEdgePartition[ED2] = {
    new GrapeEdgePartition[ED2](
      srcOids, dstOids, newData, ivEdgeNum,ovEdgeNum, vertexMapPartition)
  }

  def srcOid(i: Long) : Long = srcOids(i.toInt)

  def dstOid(i: Long) : Long = dstOids(i.toInt)

  def edgeData(i: Long) : ED = edatas(i.toInt)

  def getVertexMapPartition() = {
    vertexMapPartition
  }
}

class GrapeEdgePartitionBuilder[@specialized(Char, Long, Int, Double) ED: ClassTag](pid : Int, partitionNum : Int, partitioner : Partitioner) extends Logging{
//  private val srcOidBuffer = new ArrayBuffer[Long]
//  private val dstOidBuffer = new ArrayBuffer[Long]
//  private val edataBuffer = new ArrayBuffer[ED]
  private val edgeArray = new ArrayBuffer[Edge[ED]]
  private val innerVertexLid2Oid = new ArrayBuffer[Long]
  private val outerVertexLid2Oid = new ArrayBuffer[Long]
  private val innerVertexOid2Lid = new GraphXPrimitiveKeyOpenHashMap[VertexId, Long]
  //The lid for outer vertex also start from 0, but we should plus ivnum.
  private val outerVertexOid2Lid = new GraphXPrimitiveKeyOpenHashMap[VertexId,Long]
  private val outerVertexOid2Fid = new GraphXPrimitiveKeyOpenHashMap[VertexId,Int]

  def add(src : Long, dst : Long, edata : ED): Unit ={
    edgeArray += Edge[ED](src,dst,edata)
  }
  def add(edge: Edge[ED]): Unit ={
    edgeArray += edge
  }

  def size = edgeArray.size
  def innerVertexNum = innerVertexLid2Oid.size

//  def initInnerVertexOids(): Unit = {
//    var ind = 0;
//    val curPartitionOidSet = oidSets(pid)
//    while (ind < srcOidBuffer.size){
//      curPartitionOidSet+=srcOidBuffer(ind)
//      ind += 1
//    }
//    ind = 0
//    while (ind < dstOidBuffer.size){
//      val curPid = partitioner.getPartition(dstOidBuffer(ind))
//      val curPartitionOidSet = oidSets(curPid)
//      curPartitionOidSet += dstOidBuffer(ind)
//      log.info(s"Partition ${pid}: Dst node of edge (${srcOidBuffer(ind)}, ${dstOidBuffer(ind)} belong to ${curPid}")
//      ind += 1
//    }
//    log.info(s"Partition ${pid}: After init innerVertexOids : ${oidSets.mkString("OidSetArray(", ",", ")")}")
//    curPartitionOidSet.foreach(
//      id => innerVertexOids += id
//    )
//    log.info(s"curPartition oid list: ${innerVertexOids.toArray.mkString("Array(", ", ", ")")}")
//  }
  def myClassOf[T: ClassTag] = implicitly[ClassTag[T]].runtimeClass

  def toGrapeEdgePartition : GrapeEdgePartition[ED] = {
    log.info(s"Start convert to GrapeEdgePartition[${pid}], total num edges: ${edgeArray.size}")
    var oePointer = edgeArray.size - 1
    var iePointer = 0
    val srcOidArray = new Array[Long](edgeArray.size)
    val dstOidArray = new Array[Long](edgeArray.size)
    val edataArray = new Array[ED](edgeArray.size)
    //First generate oid arrays. the first column presents all inner vertices. The vertices that can not be partitioned into
    //pid are outer vertices. and we know where it belongs by hashing.
    var ivCurLid = -1
    var ovCurLid = -1
    var ivEdgeCnt = 0 //count the number of edges start from iv
    var ovEdgeCnt = 0//count the number of edges start from ov
    for (ind <- edgeArray.indices){
      val edge = edgeArray(ind)
      val srcOid = edge.srcId
      val dstOid = edge.dstId

      val srcOidPid = partitioner.getPartition(srcOid)
      if (srcOidPid == pid){
        innerVertexOid2Lid.changeValue(srcOid, { ivCurLid += 1 ; innerVertexLid2Oid += srcOid; ivCurLid}, identity)
        ivEdgeCnt += 1
        srcOidArray(iePointer) = srcOid
        dstOidArray(iePointer) = dstOid
        edataArray(iePointer) = edge.attr
        iePointer += 1
      }
      else {
        outerVertexOid2Lid.changeValue(srcOid, {ovCurLid += 1; outerVertexLid2Oid += srcOid; ovCurLid}, identity)
        outerVertexOid2Fid.changeValue(srcOid, srcOidPid, identity)
        ovEdgeCnt += 1
        srcOidArray(oePointer) = srcOid
        dstOidArray(oePointer) = dstOid
        edataArray(oePointer) = edge.attr
        oePointer -= 1
      }
      val dstOidPid = partitioner.getPartition(dstOid)
      if (dstOidPid == pid) {
        innerVertexOid2Lid.changeValue(dstOid, {ivCurLid += 1; innerVertexLid2Oid += dstOid; ivCurLid}, identity)
      }
      else {
        outerVertexOid2Lid.changeValue(dstOid, {ovCurLid += 1; outerVertexLid2Oid += dstOid; ovCurLid}, identity)
        outerVertexOid2Fid.changeValue(dstOid, dstOidPid, identity)
      }
    }
    require(ivEdgeCnt + ovEdgeCnt == edgeArray.size, "edge cnt doesn't match")
    require(iePointer == oePointer + 1, "two pointer now met: " + iePointer + " " +oePointer)
    val innerVertexNum = ivCurLid + 1
    val outerVertexNum = ovCurLid + 1
    log.info(s" ivnum: ${innerVertexNum}, ovnum: ${outerVertexNum}, total ${innerVertexNum + outerVertexNum}")

    val ivLid2Oid = innerVertexLid2Oid.toArray
    val ovLid2Oid = outerVertexLid2Oid.toArray
    log.info(s"Partition ${pid}: srcOidArray ${srcOidArray.mkString("Array(", ", ", ")")}")
    log.info(s"Partition ${pid}: dstOidArray ${dstOidArray.mkString("Array(", ", ", ")")}")
    log.info(s"Partition ${pid}: edataArray ${edataArray.mkString("Array(", ", ", ")")}")
    log.info(s"Partition ${pid}: ivLid2Oid ${ivLid2Oid.mkString("Array(", ", ", ")")}")
    log.info(s"Partition ${pid}: ovLid2Oid ${ovLid2Oid.mkString("Array(", ", ", ")")}")


    log.info(s"Partition ${pid}: ivOid2Lid: ${toString(innerVertexOid2Lid)}")
    log.info(s"Partition ${pid}: ovOid2Lid: ${toString(outerVertexOid2Lid)}")
    log.info(s"Partition ${pid}: ovOid2fid: ${toString(outerVertexOid2Fid)}")

    new GrapeEdgePartition[ED](srcOidArray, dstOidArray, edataArray,ivEdgeCnt, ovEdgeCnt, new GrapeVertexMapPartition(pid, ivLid2Oid, ovLid2Oid,
      innerVertexOid2Lid, outerVertexOid2Lid, outerVertexOid2Fid))
  }

  def toString(index : GraphXPrimitiveKeyOpenHashMap[VertexId,_]) : String = {
    var res = ""
    val iter = index.iterator
    while (iter.hasNext){
      val cur = iter.next()
      res += s"(${cur._1}: ${cur._2}) "
    }
    res
  }
}

/**
 * Create new EdgePartition with no more vertex added.
 */
