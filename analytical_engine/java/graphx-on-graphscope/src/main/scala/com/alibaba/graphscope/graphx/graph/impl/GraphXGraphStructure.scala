package com.alibaba.graphscope.graphx.graph.impl

import com.alibaba.graphscope.ds.{ImmutableTypedArray, Vertex}
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.{GraphStructureType, GraphXFragmentStructure}
import com.alibaba.graphscope.graphx.graph.{GSEdgeTripletImpl, GraphStructure, ReusableEdge, ReusableEdgeImpl}
import com.alibaba.graphscope.graphx.store.VertexDataStore
import com.alibaba.graphscope.graphx.utils.PrimitiveVector
import org.apache.spark.graphx.{EdgeTriplet, _}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.reflect.ensureAccessible

//import scala.collection.BitSet
import scala.reflect.ClassTag

/** the edge array only contains out edges, we use in edge as a comparison  */
class GraphXGraphStructure(val vm : GraphXVertexMap[Long,Long], val lid2Oid : Array[Long], val csr : GraphXCSR[Long]) extends GraphStructure with Logging{
  val startLid = 0
  val endLid: Long = vm.innerVertexSize()
  val oeBeginNbr = csr.getOEBegin(0)
  val oeBeginAddr = csr.getOEBegin(0).getAddress

  lazy val inDegreeArray: Array[Int] = getInDegreeArray
  lazy val outDegreeArray: Array[Int] = getOutDegreeArray
  lazy val inOutDegreeArray: Array[Int] = getInOutDegreeArray
  override val mirrorVertices: Array[Array[VertexId]] = getMirrorVertices
  lazy val eids : Array[Long] = createEids

  val oeOffsetsArray: ImmutableTypedArray[Long] = csr.getOEOffsetsArray.asInstanceOf[ImmutableTypedArray[Long]]
  val ieOffsetsArray : ImmutableTypedArray[Long] = csr.getIEOffsetsArray.asInstanceOf[ImmutableTypedArray[Long]]


  @inline
  def getOEOffset(lid : Long) : Long = {
    oeOffsetsArray.get(lid)
  }

  @inline
  def getIEOffset(lid : Long) : Long = {
    ieOffsetsArray.get(lid)
  }

  @inline
  def getOutDegree(l: Long) : Long = {
    oeOffsetsArray.get(l + 1) - oeOffsetsArray.get(l)
  }

  @inline
  def getInDegree(l: Long) : Long = {
    ieOffsetsArray.get(l + 1) - ieOffsetsArray.get(l)
  }

  private def createEids : Array[Long] = {
    val edgeNum = getOutEdgesNum
    val res = new Array[Long](edgeNum.toInt)
    var i = 0
    oeBeginNbr.setAddress(oeBeginAddr)
    while (i < edgeNum){
      res(i) = oeBeginNbr.eid()
      oeBeginNbr.addV(16)
      i += 1
    }
    res
  }

  private def getOutDegreeArray : Array[Int] = {
    val time0 = System.nanoTime()
    val len = vm.getVertexSize.toInt
    val res = new Array[Int](len)
    var i = 0
    while (i < endLid){
      res(i) = getOutDegree(i).toInt
      i += 1
    }
    while (i < len){
      res(i) = 0
      i += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Get out degree array cost ${(time1 - time0)/1000000} ms")
    res
  }

  private def getInDegreeArray : Array[Int] = {
    val time0 = System.nanoTime()
    val len = vm.getVertexSize.toInt
    val res = new Array[Int](len)
    var i = 0
    while (i < endLid){
      res(i) =  getInDegree(i).toInt
      i += 1
    }
    while (i < len){
      res(i) =  0
      i += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Get in degree array cost ${(time1 - time0)/1000000} ms")
    res
  }

  private def getInOutDegreeArray : Array[Int] = {
    val len = vm.getVertexSize.toInt
    val res = new Array[Int](len)
    var i = 0
    while (i < endLid) {
      res(i) =  getInDegree(i).toInt + getOutDegree(i).toInt
      i += 1
    }
    while (i < len) {
      res(i) =  0
      i += 1
    }
    res
  }

  private def getMirrorVertices : Array[Array[Long]] = {
    val res = new Array[PrimitiveVector[Long]](fnum())
    for (i <- res.indices){
      res(i) = new PrimitiveVector[VertexId]()
    }
    var lid = 0;
    val ivnum = vm.innerVertexSize().toInt
    val curFid = fid()
    val flags = new Array[Boolean](fnum())
    while (lid < ivnum){
      for (i <- flags.indices){
        flags(i) = false
      }
      var begin = csr.getOEBegin(lid)
      var end = csr.getOEEnd(lid)
      while (begin.getAddress < end.getAddress){
        val dstLid = begin.vid()
        val dstFid = vm.getFragId(dstLid)
        flags(dstFid) = true
        begin.addV(16)
      }
      begin = csr.getIEBegin(lid)
      end = csr.getIEEnd(lid)
      while (begin.getAddress < end.getAddress){
        val dstLid = begin.vid()
        val dstFid = vm.getFragId(dstLid)
        flags(dstFid) = true
        begin.addV(16)
      }
      for (i <- flags.indices){
        if (i != curFid && flags(i)){
          res(i).+=(lid)
        }
      }
      lid += 1
    }
    res.map(v => v.toArray)
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

  override def outerVertexGid2Vertex(gid: Long, vertex: Vertex[Long]): Boolean = vm.outerVertexGid2Vertex(gid,vertex)

  override def iterator[ED: ClassTag](edatas: Array[ED], activeEdgeSet : BitSet, edgeReversed : Boolean = false): Iterator[Edge[ED]] = {
    if (!edgeReversed){
      new Iterator[Edge[ED]] {
        var curOffset = activeEdgeSet.nextSetBit(0)
        val edge = new ReusableEdgeImpl[ED]
        var curLid = 0
        val beginAddr = csr.getOEBegin(0).getAddress
        val nbr = csr.getOEBegin(0)
        var curEndOffset = getOEOffset(curLid + 1)
        edge.srcId = lid2Oid(curLid)
        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            while (curOffset >= curEndOffset && curLid < endLid) {
              curLid += 1
              curEndOffset = getOEOffset(curLid + 1)
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
          edge.attr = edatas(nbr.eid().toInt)
          curOffset = activeEdgeSet.nextSetBit(curOffset)
          edge
        }
      }
    }
    else {
      new Iterator[Edge[ED]] {
        var curOffset = activeEdgeSet.nextSetBit(0)
        val edge = new ReusableEdgeImpl[ED]
        var curLid = 0
        val beginAddr = csr.getOEBegin(0).getAddress
        val nbr = csr.getOEBegin(0)
        var curEndOffset = getOEOffset(curLid + 1)
        edge.dstId = lid2Oid(curLid)
        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            while (curOffset >= curEndOffset && curLid < endLid) {
              curLid += 1
              curEndOffset = getOEOffset(curLid + 1)
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
          edge.attr = edatas(nbr.eid().toInt)
          curOffset = activeEdgeSet.nextSetBit(curOffset)
          edge
        }
      }
    }
  }

  override def tripletIterator[VD: ClassTag,ED : ClassTag](vertexDataStore: VertexDataStore[VD],edatas : Array[ED], activeEdgeSet : BitSet, edgeReversed : Boolean = false,
                      includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false, includeLid : Boolean = false)
  : Iterator[EdgeTriplet[VD, ED]] = {
    if (!edgeReversed){
      new Iterator[EdgeTriplet[VD, ED]] {
        var curOffset = activeEdgeSet.nextSetBit(0)
        var curLid = 0
        var srcId = 0 : Long
        val beginAddr = csr.getOEBegin(0).getAddress
        val nbr = csr.getOEBegin(0)
        var curEndOffset = getOEOffset(curLid + 1)

        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            while (curOffset >= curEndOffset && curLid < endLid) {
              curLid += 1
              curEndOffset = getOEOffset(curLid + 1) // +2 = getOeEnd of (curLid + 1)
            }
            if (curLid >= endLid) return false
            srcId = lid2Oid(curLid)
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
          edgeTriplet.srcId = srcId
          edgeTriplet.attr = edatas(edgeTriplet.eid.toInt)
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
        var curOffset = activeEdgeSet.nextSetBit(0)
        var curLid = 0
        var dstId = 0 : Long
        val beginAddr = csr.getOEBegin(0).getAddress
        val nbr = csr.getOEBegin(0)
        var curEndOffset = getOEOffset(curLid + 1)

        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            while (curOffset >= curEndOffset && curLid < endLid) {
              curLid += 1
              curEndOffset = getOEOffset(curLid + 1)
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
          edgeTriplet.dstId = dstId
          edgeTriplet.attr = edatas(edgeTriplet.eid.toInt)
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

  override def getOutNbrIds(vid: VertexId): Array[VertexId] = {
    val res = new Array[VertexId](outDegreeArray(vid.toInt))
    fillOutNbrIds(vid, res)
    res
  }

  def fillOutNbrIds(vid : VertexId, array: Array[VertexId],startInd : Int = 0) : Unit = {
    var cur = getOEOffset(vid)
    val end = getOEOffset(vid + 1)
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

  override def getInNbrIds(vid: VertexId): Array[VertexId] = {
    val res = new Array[VertexId](inDegreeArray(vid.toInt))
    fillInNbrIds(vid, res)
    res
  }

  def fillInNbrIds(vid :VertexId, array : Array[VertexId], startInd : Int = 0) : Unit = {
    val beginNbr = csr.getIEBegin(vid)
    val endNbr = csr.getIEEnd(vid)
    var i = startInd
    while (beginNbr.getAddress < endNbr.getAddress){
      array(i) = lid2Oid(beginNbr.vid().toInt)
      i += 1
      beginNbr.addV(16)
    }
  }

  override def getInOutNbrIds(vid: VertexId): Array[VertexId] = {
    val size = inDegreeArray(vid.toInt) + outDegreeArray(vid.toInt)
    val res = new Array[VertexId](size)
    fillOutNbrIds(vid, res, 0)
    fillInNbrIds(vid, res, inDegreeArray(vid.toInt))
    res
  }

  override def iterateTriplets[VD: ClassTag, ED: ClassTag,ED2 : ClassTag](f: EdgeTriplet[VD,ED] => ED2,vertexDataStore: VertexDataStore[VD], edatas: Array[ED], activeSet: BitSet, edgeReversed: Boolean, includeSrc: Boolean, includeDst: Boolean, resArray : Array[ED2]): Unit = {
    val time0 = System.nanoTime()
//    var offset: Int = activeSet.nextSetBit(0)
//    val edgeTriplet = new GSEdgeTripletImpl[VD, ED]
//
//    while (offset >= 0) {
//      edgeTriplet.srcAttr = vertexDataStore.getData(srcLids(offset))
//      edgeTriplet.dstAttr = vertexDataStore.getData(dstLids(offset))
//      edgeTriplet.srcId = srcOids(offset)
//      edgeTriplet.dstId = dstOids(offset)
//      val eid = eids(offset).toInt
//      edgeTriplet.attr = edatas(eid) //edgeTriplet.eid
//      resArray(eid) = f(edgeTriplet)
//      offset = activeSet.nextSetBit(offset + 1)
//    }
    var curLid = 0
    val nbr = csr.getOEBegin(0)
    val edgeTriplet = new GSEdgeTripletImpl[VD, ED]
    var curOffset = 0
    if (!edgeReversed){
      while (curLid < endLid){
        val curEndOffset = getOEOffset(curLid + 1)
        edgeTriplet.srcId = lid2Oid(curLid)
        edgeTriplet.srcAttr = vertexDataStore.getData(curLid)
        while (curOffset < curEndOffset){
          if (activeSet.get(curOffset)){
            val dstLid = nbr.vid()
            edgeTriplet.dstId = lid2Oid(dstLid.toInt)
            edgeTriplet.dstAttr = vertexDataStore.getData(dstLid)
            edgeTriplet.attr = edatas(nbr.eid().toInt)
          }
          curOffset += 1
          nbr.addV(16)
        }
        curLid += 1
      }
    }
    else {
      while (curLid < endLid){
        val curEndOffset = getOEOffset(curLid + 1)
        edgeTriplet.dstId = lid2Oid(curLid)
        edgeTriplet.dstAttr = vertexDataStore.getData(curLid)
        while (curOffset < curEndOffset){
          if (activeSet.get(curOffset)){
            val dstLid = nbr.vid()
            edgeTriplet.srcId = lid2Oid(dstLid.toInt)
            edgeTriplet.srcAttr = vertexDataStore.getData(dstLid)
            edgeTriplet.attr = edatas(nbr.eid().toInt)
          }
          curOffset += 1
          nbr.addV(16)
        }
        curLid += 1
      }
    }


    val time1 = System.nanoTime()
    log.info(s"[GraphXGraphStructure:] iterating over edges triplet cost ${(time1 - time0)/ 1000000}ms")
  }

  override def iterateEdges[ED: ClassTag, ED2: ClassTag](f: Edge[ED] => ED2,edatas : Array[ED], activeSet: BitSet, edgeReversed: Boolean, newArray: Array[ED2]): Unit = {
    val time0 = System.nanoTime()
//    var offset: Int = activeSet.nextSetBit(0)
//    val edge = new ReusableEdgeImpl[ED]
//
//    while (offset >= 0) {
//      edge.srcId = srcOids(offset)
//      edge.dstId = dstOids(offset)
//      val eid = eids(offset).toInt
//      edge.attr = edatas(eid)
//      newArray(eid) = f(edge)
//      offset = activeSet.nextSetBit(offset + 1)
//    }
    var curLid = 0
    val nbr = csr.getOEBegin(0)
    val edge = new ReusableEdgeImpl[ED]
    var curOffset = 0
    if (!edgeReversed){
      while (curLid < endLid){
        val curEndOffset = getOEOffset(curLid + 1)
        edge.srcId = lid2Oid(curLid)
        while (curOffset < curEndOffset){
          if (activeSet.get(curOffset)){
            val dstLid = nbr.vid()
            edge.dstId = lid2Oid(dstLid.toInt)
            edge.attr = edatas(nbr.eid().toInt)
          }
          curOffset += 1
          nbr.addV(16)
        }
        curLid += 1
      }
    }
    else {
      while (curLid < endLid){
        val curEndOffset = getOEOffset(curLid + 1)
        edge.dstId = lid2Oid(curLid)
        while (curOffset < curEndOffset){
          if (activeSet.get(curOffset)){
            val dstLid = nbr.vid()
            edge.srcId = lid2Oid(dstLid.toInt)
            edge.attr = edatas(nbr.eid().toInt)
          }
          curOffset += 1
          nbr.addV(16)
        }
        curLid += 1
      }
    }


    val time1 = System.nanoTime()
    log.info(s"[GraphXGraphStructure:] iterating over edges cost ${(time1 - time0)/ 1000000}ms")
  }
}

