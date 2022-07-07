package com.alibaba.graphscope.graphx.graph.impl

import com.alibaba.graphscope.ds.{ImmutableTypedArray, Vertex}
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.{GraphStructureType, GraphXFragmentStructure}
import com.alibaba.graphscope.graphx.graph.{GSEdgeTripletImpl, GraphStructure, ReusableEdge, ReusableEdgeImpl}
import com.alibaba.graphscope.graphx.store.VertexDataStore
import com.alibaba.graphscope.graphx.utils.PrimitiveVector
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

//import scala.collection.BitSet
import scala.reflect.ClassTag

/** the edge array only contains out edges, we use in edge as a comparison  */
class GraphXGraphStructure(val vm : GraphXVertexMap[Long,Long], val csr : GraphXCSR[Long],  var srcLids : Array[Long],
                           val dstLids : Array[Long], val srcOids : Array[Long],
                           val dstOids : Array[Long],
                           val eids : Array[Long]) extends GraphStructure with Logging{
  val startLid = 0
  val endLid: Long = vm.innerVertexSize()

  lazy val inDegreeArray: Array[Int] = getInDegreeArray
  lazy val outDegreeArray: Array[Int] = getOutDegreeArray
  lazy val inOutDegreeArray: Array[Int] = getInOutDegreeArray
  override val mirrorVertices: Array[Array[VertexId]] = getMirrorVertices

  val oeOffsetsArray: ImmutableTypedArray[Long] = csr.getOEOffsetsArray.asInstanceOf[ImmutableTypedArray[Long]]
  val ieOffsetsArray : ImmutableTypedArray[Long] = csr.getIEOffsetsArray.asInstanceOf[ImmutableTypedArray[Long]]

  val lid2Oid : Array[Long] = {
    val res = new Array[Long](vm.getVertexSize.toInt)
    var i = 0;
    val limit = vm.getVertexSize.toInt
    while (i < limit){
      res(i) = vm.getId(i)
      i += 1
    }
    res
  }


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

  override def innerVertexLid2Oid(lid: Long): Long = vm.innerVertexLid2Oid(lid)

  override def outerVertexLid2Oid(lid: Long): Long = vm.outerVertexLid2Oid(lid)

  override def getOuterVertexSize: Long = vm.getOuterVertexSize

  override def innerOid2Gid(oid: Long): Long = vm.innerOid2Gid(oid)

  override def getOuterVertexGid(lid: Long): Long = vm.getOuterVertexGid(lid)

  override def fid2GraphxPid(fid: Int): Int = vm.fid2GraphxPid(fid)

  override def outerVertexGid2Vertex(gid: Long, vertex: Vertex[Long]): Boolean = vm.outerVertexGid2Vertex(gid,vertex)

  override def iterator[ED: ClassTag](edatas: Array[ED], activeEdgeSet : BitSet, edgeReversed : Boolean = false): Iterator[Edge[ED]] = {
    if (edgeReversed){
      new Iterator[Edge[ED]] {
        var offset: Int = activeEdgeSet.nextSetBit(0)
        val edge = new ReusableEdgeImpl[ED];

        override def hasNext: Boolean = {
          offset >= 0
        }

        override def next(): Edge[ED] = {
          edge.dstId = srcOids(offset)
          edge.srcId = dstOids(offset)
          edge.attr = edatas(eids(offset).toInt)
          edge.offset = offset
          edge.eid = eids(offset)
          offset = activeEdgeSet.nextSetBit(offset.toInt + 1)
          edge
        }
      }
    }
    else {
      new Iterator[Edge[ED]] {
        var offset: Int = activeEdgeSet.nextSetBit(0)
        val edge: ReusableEdge[ED] = new ReusableEdgeImpl[ED];
        override def hasNext: Boolean = {
          offset >= 0
        }

        override def next(): Edge[ED] = {
          edge.srcId = srcOids(offset)
          edge.dstId = dstOids(offset)
          edge.attr = edatas(eids(offset).toInt)
          edge.offset = offset
          edge.eid = eids(offset)
          offset = activeEdgeSet.nextSetBit(offset.toInt + 1)
          edge
        }
      }
    }
  }

  override def tripletIterator[VD: ClassTag,ED : ClassTag](vertexDataStore: VertexDataStore[VD],edatas : Array[ED], activeEdgeSet : BitSet, edgeReversed : Boolean = false,
                      includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false)
  : Iterator[EdgeTriplet[VD, ED]] = {
    //For input vertex data store, check its version, if its version is greater than us, update
    //vertex data. If equal, skip. less than us is impossible.
//    updateVertexDataCache(vertexDataStore)
    if (edgeReversed){
      new Iterator[EdgeTriplet[VD, ED]] {
        var offset: Int = activeEdgeSet.nextSetBit(0)
        val reuseEdgeTriplet = new GSEdgeTripletImpl[VD, ED];
        override def hasNext: Boolean = {
          offset >= 0
        }

        override def next(): EdgeTriplet[VD, ED] = {
          if (!reuseTriplet){
            val edgeTriplet = new GSEdgeTripletImpl[VD, ED];
            edgeTriplet.eid = eids(offset)
            edgeTriplet.offset = offset
            edgeTriplet.dstId = srcOids(offset)
            edgeTriplet.srcId = dstOids(offset)
            edgeTriplet.attr = edatas(edgeTriplet.eid.toInt)
            //          edgeTriplet.dstAttr = vertexDataStore.getData(srcLids.get(offset))
            //          edgeTriplet.srcAttr = vertexDataStore.getData(dstLids.get(offset))
            offset = activeEdgeSet.nextSetBit(offset.toInt + 1)
            edgeTriplet
          }
          else {
            reuseEdgeTriplet.eid = eids(offset)
            reuseEdgeTriplet.offset = offset
            reuseEdgeTriplet.dstId = srcOids(offset)
            reuseEdgeTriplet.srcId = dstOids(offset)
            reuseEdgeTriplet.attr = edatas(reuseEdgeTriplet.eid.toInt)
            //          edgeTriplet.dstAttr = vertexDataStore.getData(srcLids.get(offset))
            //          edgeTriplet.srcAttr = vertexDataStore.getData(dstLids.get(offset))
            offset = activeEdgeSet.nextSetBit(offset.toInt + 1)
            reuseEdgeTriplet
          }

        }
      }
    } else {
      new Iterator[EdgeTriplet[VD, ED]] {
        var offset: Int = activeEdgeSet.nextSetBit(0)
        val reuseEdgeTriplet = new GSEdgeTripletImpl[VD, ED]
        override def hasNext: Boolean = {
          offset >= 0
        }

        override def next(): EdgeTriplet[VD, ED] = {
          if (!reuseTriplet){
            val edgeTriplet = new GSEdgeTripletImpl[VD, ED]
            edgeTriplet.eid = eids(offset)
            edgeTriplet.offset = offset
            edgeTriplet.srcId = srcOids(offset)
            edgeTriplet.dstId = dstOids(offset)
            edgeTriplet.attr = edatas(offset) //edgeTriplet.eid
            edgeTriplet.srcAttr = vertexDataStore.getData(srcLids(offset))
            edgeTriplet.dstAttr = vertexDataStore.getData(dstLids(offset))
            offset = activeEdgeSet.nextSetBit(offset.toInt + 1)
            edgeTriplet
          }
          else {
            reuseEdgeTriplet.eid = eids(offset)
            reuseEdgeTriplet.offset = offset
            reuseEdgeTriplet.srcId = srcOids(offset)
            reuseEdgeTriplet.dstId = dstOids(offset)
            reuseEdgeTriplet.attr = edatas(offset) //reuseEdgeTriplet.eid
            reuseEdgeTriplet.srcAttr = vertexDataStore.getData(srcLids(offset))
            reuseEdgeTriplet.dstAttr = vertexDataStore.getData(dstLids(offset))
            offset = activeEdgeSet.nextSetBit(offset.toInt + 1)
            reuseEdgeTriplet
          }
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
    var i = startInd
    while (cur < end){
      array(i) = dstOids(cur.toInt)
      cur += 1
      i += 1
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
}

