package com.alibaba.graphscope.graphx.graph.impl

import com.alibaba.graphscope.ds.{ImmutableTypedArray, Vertex}
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.{GraphStructureType, GraphXFragmentStructure}
import com.alibaba.graphscope.graphx.graph.{GSEdgeTripletImpl, GraphStructure, ReusableEdgeImpl}
import com.alibaba.graphscope.graphx.store.VertexDataStore
import com.alibaba.graphscope.graphx.utils.{ArrayWithOffset, BitSetWithOffset, PrimitiveVector}
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

/** the edge array only contains out edges, we use in edge as a comparison  */
class GraphXGraphStructure(val vm : GraphXVertexMap[Long,Long], val lid2Oid : Array[Long], val eids : Array[Long], val csr : GraphXCSR[Long]) extends GraphStructure with Logging{
  val oeBeginNbr = csr.getOEBegin(0)
  val oeBeginAddr = csr.getOEBegin(0).getAddress
  val ivnum = vm.innerVertexSize()

//  lazy val inDegreeArray: Array[Int] = getInDegreeArray
//  lazy val outDegreeArray: Array[Int] = getOutDegreeArray
//  lazy val inOutDegreeArray: Array[Int] = getInOutDegreeArray
  //FIXME: bitset for long
  override val mirrorVertices: Array[BitSet] = getMirrorVertices

  val oeOffsetsArray: Array[Long] = {
    val tmp : ImmutableTypedArray[Long] = csr.getOEOffsetsArray.asInstanceOf[ImmutableTypedArray[Long]]
    val res = new Array[Long](tmp.getLength.toInt)
    require(res.length == ivnum)
    var i = 0
    while (i < res.length){
      res(i) = tmp.get(i)
      i += 1
    }
    res
  }
  val ieOffsetsArray : Array[Long] = {
    val tmp : ImmutableTypedArray[Long] = csr.getIEOffsetsArray.asInstanceOf[ImmutableTypedArray[Long]]
    val res = new Array[Long](tmp.getLength.toInt)
    require(res.length == ivnum)
    var i = 0
    while (i < res.length){
      res(i) = tmp.get(i)
      i += 1
    }
    res
  }

  val dstOids : Array[Long] = new Array[Long](csr.getOutEdgesNum.toInt)
  val dstLids : Array[Int] = new Array[Int](csr.getOutEdgesNum.toInt)
  def init() = {
    val nbr = csr.getOEBegin(0)
    var offset = 0
    val limit = csr.getOEOffset(ivnum)
    while (offset < limit){
      val dstLid = nbr.vid().toInt
      dstOids(offset) = lid2Oid(dstLid)
      dstLids(offset) = dstLid
      offset += 1
      nbr.addV(16)
    }
  }
  init()


  @inline
  override def getOEBeginOffset(lid : Int) : Long = {
    oeOffsetsArray(lid)
  }

  @inline
  override def getIEBeginOffset(lid : Int) : Long = {
    ieOffsetsArray(lid)
  }

  @inline
  override def getOEEndOffset(lid : Int) : Long = {
    oeOffsetsArray(lid + 1)
  }

  @inline
  override def getIEEndOffset(lid : Int) : Long = {
    ieOffsetsArray(lid + 1)
  }

  @inline
  def getOutDegree(l: Int) : Long = {
    oeOffsetsArray(l + 1) - oeOffsetsArray(l)
  }

  @inline
  def getInDegree(l: Int) : Long = {
    ieOffsetsArray(l + 1) - ieOffsetsArray(l)
  }

  def outDegreeArray(startLid : Long, endLid : Long) : Array[Int] = {
    val time0 = System.nanoTime()
    val len = endLid - startLid
    val res = new Array[Int](len.toInt)
    var i = startLid.toInt
    while (i < endLid){
      res(i - startLid.toInt) = getOutDegree(i).toInt
      i += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Get out degree array cost ${(time1 - time0)/1000000} ms")
    res
  }

  /** array length equal (end-start), indexed with 0 */
  def inDegreeArray(startLid : Long, endLid : Long) : Array[Int] = {
    val time0 = System.nanoTime()
    val len = endLid - startLid
    val res = new Array[Int](len.toInt)
    var i = startLid.toInt
    while (i < endLid){
      res(i - startLid.toInt) = getInDegree(i).toInt
      i += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Get in degree array cost ${(time1 - time0)/1000000} ms")
    res
  }

  def inOutDegreeArray(startLid : Long, endLid : Long) : Array[Int] = {
    val len = endLid - startLid
    val res = new Array[Int](len.toInt)
    var i = startLid.toInt
    while (i < endLid) {
      res(i -startLid.toInt) = getInDegree(i).toInt + getOutDegree(i).toInt
      i += 1
    }
    res
  }

  private def getMirrorVertices : Array[BitSet] = {
    val res = new Array[BitSet](fnum())
    val ivnum = vm.innerVertexSize().toInt
    for (i <- res.indices){
      res(i) = new BitSet(ivnum)
    }
    var lid = 0;
    val curFid = fid()
    val flags = new Array[Boolean](fnum())
    while (lid < ivnum){
      for (i <- flags.indices){
        flags(i) = false
      }
//      var begin = csr.getOEBegin(lid)
//      var end = csr.getOEEnd(lid)
//      while (begin.getAddress < end.getAddress){
//        val dstLid = begin.vid()
//        val dstFid = vm.getFragId(dstLid)
//        flags(dstFid) = true
//        begin.addV(16)
//      }
      //only in edges are enough
      val begin = csr.getIEBegin(lid)
      val end = csr.getIEEnd(lid)
      while (begin.getAddress < end.getAddress){
        val dstLid = begin.vid()
        val dstFid = vm.getFragId(dstLid)
        flags(dstFid) = true
        begin.addV(16)
      }
      for (i <- flags.indices){
        if (i != curFid && flags(i)){
          res(i).set(lid)
        }
      }
      lid += 1
    }
    res
  }

//  override def getInDegree(vid: Long): Long = csr.getInDegree(vid)
//
//  override def getOutDegree(vid: Long): Long = csr.getOutDegree(vid)

  override def isInEdgesEmpty(vid: Long): Boolean = csr.isInEdgesEmpty(vid)

  override def isOutEdgesEmpty(vid: Long): Boolean = csr.isOutEdgesEmpty(vid)

  override def getInEdgesNum: Long = csr.getInEdgesNum

  override def getOutEdgesNum: Long = csr.getOutEdgesNum

  override def fid(): Int = vm.fid()

  override def fnum(): Int = vm.fnum()

  override def getId(vertex: Long): Long = lid2Oid(vertex.toInt)

  override def getVertex(oid: Long, vertex: Vertex[Long]): Boolean = vm.getVertex(oid,vertex)

  override def getTotalVertexSize: Long = vm.getTotalVertexSize

  override def getVertexSize: Long = vm.getVertexSize

  override def getInnerVertexSize: Long = vm.innerVertexSize()

  override def innerVertexLid2Oid(lid: Long): Long = lid2Oid(lid.toInt)

  override def outerVertexLid2Oid(lid: Long): Long = lid2Oid(lid.toInt)

  override def getOuterVertexSize: Long = vm.getOuterVertexSize

  override def innerOid2Gid(oid: Long): Long = vm.innerOid2Gid(oid)

  override def getOuterVertexGid(lid: Long): Long = vm.getOuterVertexGid(lid)

  override def fid2GraphxPid(fid: Int): Int = vm.fid2GraphxPid(fid)

  //FIXME: accelerate this.
  override def outerVertexGid2Vertex(gid: Long, vertex: Vertex[Long]): Boolean = vm.outerVertexGid2Vertex(gid,vertex)

  override def iterator[ED: ClassTag](startLid : Long, endLid : Long, edatas: ArrayWithOffset[ED], activeEdgeSet : BitSetWithOffset, edgeReversed : Boolean = false): Iterator[Edge[ED]] = {
    if (!edgeReversed){
      new Iterator[Edge[ED]] {
        var curOffset = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
        val edge = new ReusableEdgeImpl[ED]
        var curLid = startLid.toInt
        val beginAddr = csr.getOEBegin(0).getAddress
        val nbr = csr.getOEBegin(0)
        var curEndOffset = getOEEndOffset(curLid)
        edge.srcId = lid2Oid(curLid)
        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            if (curOffset < 0) return false
            while (curOffset >= curEndOffset && curLid < endLid) {
              curLid += 1
              curEndOffset = getOEEndOffset(curLid)
            }
            if (curLid >= endLid) return false
            edge.srcId = lid2Oid(curLid)
            true
          }
        }

        override def next(): Edge[ED] = {
          nbr.setAddress(beginAddr + curOffset * 16)
          val dstLid = nbr.vid()
          edge.dstId = lid2Oid(dstLid.toInt)
          edge.attr = edatas(curOffset)
          curOffset = activeEdgeSet.nextSetBit(curOffset + 1)
          edge
        }
      }
    }
    else {
      new Iterator[Edge[ED]] {
        var curOffset = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
        val edge = new ReusableEdgeImpl[ED]
        var curLid = startLid.toInt
        val beginAddr = csr.getOEBegin(0).getAddress
        val nbr = csr.getOEBegin(0)
        var curEndOffset = getOEEndOffset(curLid)
        edge.dstId = lid2Oid(curLid)
        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            if (curOffset < 0) return false
            while (curOffset >= curEndOffset && curLid < endLid) {
              curLid += 1
              curEndOffset = getOEEndOffset(curLid)
            }
            if (curLid >= endLid) return false
            edge.srcId = lid2Oid(curLid)
            true
          }
        }

        override def next(): Edge[ED] = {
          nbr.setAddress(beginAddr + curOffset * 16)
          val dstLid = nbr.vid()
          edge.srcId = lid2Oid(dstLid.toInt)
          edge.attr = edatas(curOffset)
          curOffset = activeEdgeSet.nextSetBit(curOffset + 1)
          edge
        }
      }
    }
  }

  override def tripletIterator[VD: ClassTag,ED : ClassTag](startLid : Long, endLid : Long,innerVertexDataStore: VertexDataStore[VD],outerVertexDataStore: VertexDataStore[VD],edatas : ArrayWithOffset[ED], activeEdgeSet : BitSetWithOffset, edgeReversed : Boolean = false,
                      includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false, includeLid : Boolean = false)
  : Iterator[EdgeTriplet[VD, ED]] = {
    if (!edgeReversed){
      new Iterator[EdgeTriplet[VD, ED]] {
        var curOffset = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
        var curLid = startLid.toInt
        var srcId = lid2Oid(curLid)
        var srcAttr :VD = innerVertexDataStore.getData(curLid)
        val beginAddr = csr.getOEBegin(0).getAddress
        val nbr = csr.getOEBegin(0)
        var curEndOffset = getOEEndOffset(curLid)

        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            if (curOffset < 0) return false
            while (curOffset >= curEndOffset && curLid < endLid) {
              curLid += 1
              curEndOffset = getOEEndOffset(curLid)
            }
            if (curLid >= endLid) return false
            srcId = lid2Oid(curLid)
            srcAttr = innerVertexDataStore.getData(curLid)
            true
          }
        }

        override def next(): EdgeTriplet[VD, ED] = {
          val edgeTriplet = new GSEdgeTripletImpl[VD, ED];
          nbr.setAddress(beginAddr + curOffset * 16)
          val dstLid = nbr.vid().toInt
          edgeTriplet.eid = nbr.eid()
          edgeTriplet.offset = curOffset
          edgeTriplet.dstId = lid2Oid(dstLid)
          edgeTriplet.dstAttr = {
            if (dstLid >=ivnum){
              outerVertexDataStore.getData(dstLid)
            }
            else innerVertexDataStore.getData(dstLid)
          }
          edgeTriplet.srcId = srcId
          edgeTriplet.srcAttr = srcAttr
          edgeTriplet.attr = edatas(curOffset)
          if (includeLid){
            edgeTriplet.srcLid = curLid
            edgeTriplet.dstLid = dstLid
          }
          curOffset = activeEdgeSet.nextSetBit(curOffset + 1)
          edgeTriplet
          }
        }
    } else {
      new Iterator[EdgeTriplet[VD, ED]] {
        var curOffset = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
        var curLid = 0
        var dstId = lid2Oid(curLid)
        var dstAttr :VD = innerVertexDataStore.getData(curLid)
        val beginAddr = csr.getOEBegin(0).getAddress
        val nbr = csr.getOEBegin(0)
        var curEndOffset = getOEEndOffset(curLid)

        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            if (curOffset < 0) return false
            while (curOffset >= curEndOffset && curLid < endLid) {
              curLid += 1
              curEndOffset = getOEEndOffset(curLid)
            }
            if (curLid >= endLid) return false
            dstId = lid2Oid(curLid)
            true
          }
        }

        override def next(): EdgeTriplet[VD, ED] = {
          val edgeTriplet = new GSEdgeTripletImpl[VD, ED];
          nbr.setAddress(beginAddr + curOffset * 16)
          val srcLid = nbr.vid().toInt
          edgeTriplet.eid = nbr.eid()
          edgeTriplet.offset = curOffset
          edgeTriplet.srcId = lid2Oid(srcLid)
          edgeTriplet.srcAttr = {
            if (srcLid >= ivnum){
              outerVertexDataStore.getData(srcLid)
            }
            else innerVertexDataStore.getData(srcLid)
          }
          edgeTriplet.dstId = dstId
          edgeTriplet.dstAttr = dstAttr
          edgeTriplet.attr = edatas(curOffset)
          if (includeLid){
            edgeTriplet.dstLid = curLid
            edgeTriplet.srcLid = srcLid
          }
          curOffset = activeEdgeSet.nextSetBit(curOffset + 1)
          edgeTriplet
        }
      }
    }
  }

  override def getInnerVertex(oid: Long, vertex: Vertex[Long]): Boolean = {
    require(vm.getVertex(oid, vertex))
    require(vertex.GetValue() < vm.innerVertexSize())
    true
  }

  override val structureType: GraphStructureType = GraphXFragmentStructure

  override def getEids: Array[VertexId] = eids

  override def getOutNbrIds(vid: Int): Array[VertexId] = {
    val res = new Array[VertexId](getOutDegree(vid.toInt).toInt)
    fillOutNbrIds(vid, res)
    res
  }

  def fillOutNbrIds(vid : Int, array: Array[VertexId],startInd : Int = 0) : Unit = {
    var cur = getOEBeginOffset(vid)
    val end = getOEEndOffset(vid)
    oeBeginNbr.setAddress(cur * 16 + oeBeginAddr)
    var i = startInd
    while (cur < end){
      val lid = oeBeginNbr.vid()
      array(i) = lid2Oid(lid.toInt)
      cur += 1
      i += 1
      oeBeginNbr.addV(16)
    }
  }

  override def getInNbrIds(vid: Int): Array[VertexId] = {
    val res = new Array[VertexId](getInDegree(vid.toInt).toInt)
    fillInNbrIds(vid, res)
    res
  }

  def fillInNbrIds(vid :Int, array : Array[VertexId], startInd : Int = 0) : Unit = {
    val beginNbr = csr.getIEBegin(vid)
    val endNbr = csr.getIEEnd(vid)
    var i = startInd
    while (beginNbr.getAddress < endNbr.getAddress){
      array(i) = lid2Oid(beginNbr.vid().toInt)
      i += 1
      beginNbr.addV(16)
    }
  }

  override def getInOutNbrIds(vid: Int): Array[VertexId] = {
    val size = getInDegree(vid.toInt) + getOutDegree(vid.toInt)
    val res = new Array[VertexId](size.toInt)
    fillOutNbrIds(vid, res, 0)
    fillInNbrIds(vid, res, getInDegree(vid.toInt).toInt)
    res
  }

  override def iterateTriplets[VD: ClassTag, ED: ClassTag,ED2 : ClassTag](startLid : Long, endLid : Long,f: EdgeTriplet[VD,ED] => ED2,innerVertexDataStore: VertexDataStore[VD],outerVertexDataStore: VertexDataStore[VD], edatas: ArrayWithOffset[ED], activeSet: BitSetWithOffset, edgeReversed: Boolean, includeSrc: Boolean, includeDst: Boolean, resArray : ArrayWithOffset[ED2]): Unit = {
    val time0 = System.nanoTime()

    var curLid = startLid.toInt
    val edgeTriplet = new GSEdgeTripletImpl[VD, ED]
    var curOffset = activeSet.nextSetBit(activeSet.startBit)
    if (!edgeReversed){
      while (curLid < endLid && curOffset >= 0){
        val curEndOffset = getOEEndOffset(curLid)
        edgeTriplet.srcId = lid2Oid(curLid)
        edgeTriplet.srcAttr = innerVertexDataStore.getData(curLid)
        while (curOffset < curEndOffset && curOffset >= 0){
          edgeTriplet.dstId = dstOids(curOffset)
          val dstLid = dstLids(curOffset)
          if (dstLid >= ivnum){
            edgeTriplet.dstAttr = outerVertexDataStore.getData(dstLid)
          }
          else {
            edgeTriplet.dstAttr = innerVertexDataStore.getData(dstLid)
          }
          edgeTriplet.attr = edatas(curOffset)
          resArray(curOffset) = f(edgeTriplet)
          curOffset = activeSet.nextSetBit(curOffset + 1)
        }
        curLid += 1
      }
    }
    else {
      while (curLid < endLid && curOffset >= 0){
        val curEndOffset = getOEEndOffset(curLid)
        edgeTriplet.dstId = lid2Oid(curLid)
        edgeTriplet.dstAttr = innerVertexDataStore.getData(curLid)
        while (curOffset < curEndOffset && curOffset >= 0){
          edgeTriplet.srcId = dstOids(curOffset)
          val dstLid = dstLids(curOffset)
          if (dstLid >= ivnum){
            edgeTriplet.srcAttr = outerVertexDataStore.getData(dstLid)
          }
          else {
            edgeTriplet.srcAttr = innerVertexDataStore.getData(dstLid)
          }
          edgeTriplet.attr = edatas(curOffset)
          resArray(curOffset) = f(edgeTriplet)
          curOffset = activeSet.nextSetBit(curOffset + 1)
        }
        curLid += 1
      }
    }

    val time1 = System.nanoTime()
//    log.info(s"[GraphXGraphStructure:] iterating over edges triplet cost ${(time1 - time0)/ 1000000}ms")
  }

  override def iterateEdges[ED: ClassTag, ED2: ClassTag](startLid : Long, endLid : Long,f: Edge[ED] => ED2,edatas : ArrayWithOffset[ED], activeSet: BitSetWithOffset, edgeReversed: Boolean, newArray: ArrayWithOffset[ED2]): Unit = {
    val time0 = System.nanoTime()
    var curLid = startLid.toInt
    val edge = new ReusableEdgeImpl[ED]
    var curOffset = activeSet.nextSetBit(activeSet.startBit)
    if (!edgeReversed){
      while (curLid < endLid && curOffset >= 0){
        val curEndOffset = getOEEndOffset(curLid)
        edge.srcId = lid2Oid(curLid)
        while (curOffset < curEndOffset && curOffset >= 0){
          edge.dstId = dstOids(curOffset)
          edge.attr = edatas(curOffset)
          newArray(curOffset) = f(edge)
          curOffset = activeSet.nextSetBit(curOffset + 1)
        }
        curLid += 1
      }
    }
    else {
      while (curLid < endLid && curOffset >= 0){
        val curEndOffset = getOEEndOffset(curLid)
        edge.dstId = lid2Oid(curLid)
        while (curOffset < curEndOffset && curOffset >= 0){
          edge.srcId = dstOids(curOffset)
          edge.attr = edatas(curOffset)
          newArray(curOffset) = f(edge)
          curOffset = activeSet.nextSetBit(curOffset + 1)
        }
        curLid += 1
      }
    }

    val time1 = System.nanoTime()
//    log.info(s"[GraphXGraphStructure:] iterating over edges cost ${(time1 - time0)/ 1000000}ms")
  }

  override def getOEOffsetRange(startLid: VertexId, endLid: VertexId): (VertexId, VertexId) = {
    (csr.getOEOffset(startLid),csr.getOEOffset(endLid))
  }
}

