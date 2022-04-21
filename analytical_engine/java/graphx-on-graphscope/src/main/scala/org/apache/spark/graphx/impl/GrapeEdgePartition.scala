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
class GrapeEdgePartition[@specialized(Char, Long, Int, Double) Long: ClassTag, ED: ClassTag]
(srcOids: Array[Long], dstOids: Array[Long], edatas: Array[ED], oid2Lid : GraphXPrimitiveKeyOpenHashMap[VertexId,Long],
 innerVertexOids : Array[Long], oidSets: Array[mutable.HashSet[Long]], innerVertexNum : Long, outerVertexNum : Long) extends Serializable {

  val size = srcOids.length
}

class GrapeEdgePartitionBuilder[@specialized(Char, Long, Int, Double) ED: ClassTag](pid : Int, partitionNum : Int, partitioner : Partitioner) extends Logging{
  private val srcOidBuffer = new ArrayBuffer[Long]
  private val dstOidBuffer = new ArrayBuffer[Long]
  private val edataBuffer = new ArrayBuffer[ED]
  private val srcGids = new ArrayBuffer[Long]
  private val dstGids = new ArrayBuffer[Long]
  private val innerVertexOids = new ArrayBuffer[Long]
  private val oid2Lid = new GraphXPrimitiveKeyOpenHashMap[VertexId, Long]
  private val oidSets = new Array[mutable.HashSet[Long]](partitionNum)
  for (i <- 0 until oidSets.size){
    oidSets(i) = new mutable.HashSet[Long]
  }
//  private val ivOidSet = new mutable.HashSet[Int]

  def add(src : Long, dst : Long, edata : ED): Unit ={
    srcOidBuffer += src
    dstOidBuffer += dst
    edataBuffer += edata
  }
  def add(edge: Edge[ED]): Unit ={
    srcOidBuffer += edge.srcId
    dstOidBuffer += edge.dstId
    edataBuffer += edge.attr
  }

  def size = srcOidBuffer.size
  def innerVertexNum = innerVertexOids.size

  def initInnerVertexOids(): Unit = {
    var ind = 0;
    val curPartitionOidSet = oidSets(pid)
    while (ind < srcOidBuffer.size){
      curPartitionOidSet+=srcOidBuffer(ind)
      ind += 1
    }
    ind = 0
    while (ind < dstOidBuffer.size){
      val curPid = partitioner.getPartition(dstOidBuffer(ind))
      val curPartitionOidSet = oidSets(curPid)
      curPartitionOidSet += dstOidBuffer(ind)
      log.info(s"Partition ${pid}: Dst node of edge (${srcOidBuffer(ind)}, ${dstOidBuffer(ind)} belong to ${curPid}")
      ind += 1
    }
    log.info(s"Partition ${pid}: After init innerVertexOids : ${oidSets.mkString("OidSetArray(", ",", ")")}")
    curPartitionOidSet.foreach(
      id => innerVertexOids += id
    )
    log.info(s"curPartition oid list: ${innerVertexOids.toArray.mkString("Array(", ", ", ")")}")
  }
  def myClassOf[T: ClassTag] = implicitly[ClassTag[T]].runtimeClass

  def toGrapeEdgePartition : GrapeEdgePartition[Long,ED] = {
    log.info(s"Start convert to GrapeEdgePartition[${pid}], total size: ${srcOidBuffer.size}")
    //First generate oid arrays. the first column presents all inner vertices. The vertices that can not be partitioned into
    //pid are outer vertices. and we know where it belongs by hashing.
    initInnerVertexOids()

    val innerVertexNum = innerVertexOids.size
    log.info(s"Oid array for partition: ${pid} is  ${innerVertexOids.toString()}, size ${innerVertexNum}")
    log.info(s"total vertices appeard in this partition: ${countAllVertices}, total edges: ${srcOidBuffer.size}")
    var curLid = 0L
    val iter = oidSets(pid).iterator
    while (iter.hasNext){
      oid2Lid.update(iter.next(), curLid)
      curLid+=1
    }
    //Add oid<->lid mapping for outer vertices.
    for (ind <- 0 until oidSets.length){
      if (ind != pid){
        val outerOidSetIter = oidSets(ind).iterator
        while (outerOidSetIter.hasNext){
          oid2Lid.update(outerOidSetIter.next(), curLid)
          curLid += 1
        }
      }
    }
    //FIXME: Global id should be consistent among fragment, so the gid here we can generate should never
    // be used.
    log.info(toString(oid2Lid))

//    val idParser = new IdParser(pid, partitionNum)
    new GrapeEdgePartition[Long,ED](srcOidBuffer.toArray, dstOidBuffer.toArray, edataBuffer.toArray,
      oid2Lid, innerVertexOids.toArray, oidSets, innerVertexNum, countAllVertices)
  }

  def countAllVertices : Int = {
    var res = 0
    for (pSet <- oidSets){
      res += pSet.size
    }
    res
  }

  def toString(index : GraphXPrimitiveKeyOpenHashMap[VertexId,Long]) : String = {
    var res = s"Partition ${pid}: oid2Lid: "
    val iter = index.iterator
    while (iter.hasNext){
      val cur = iter.next()
      res += s"(${cur._1}: ${cur._2}) "
    }
    res
  }
}
