package com.alibaba.graphscope.graphx.graph.impl

import com.alibaba.graphscope.ds.{PropertyNbrUnit, TypedArray, Vertex}
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor
import com.alibaba.graphscope.fragment.{ArrowProjectedFragment, FragmentType, IFragment}
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.{ArrowProjectedStructure, GraphStructureType}
import com.alibaba.graphscope.graphx.graph.{GSEdgeTripletImpl, GraphStructure, ReusableEdgeImpl}
import com.alibaba.graphscope.graphx.store.VertexDataStore
import com.alibaba.graphscope.graphx.utils.{ArrayWithOffset, BitSetWithOffset}
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

class FragmentStructure(val fragment : IFragment[Long,Long,_,_]) extends GraphStructure with Logging with Serializable {
  val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
  val fid2Pid = new Array[Int](fragment.fnum())
  val ivnum = fragment.getInnerVerticesNum

  var oePtr,iePtr : PropertyNbrUnit[Long] = null
  var oePtrStartAddr,iePtrStartAddr : Long = 0
  var oeOffsetBeginArray,ieOffsetBeginArray : TypedArray[Long] = null
  var oeOffsetEndArray,ieOffsetEndArray : TypedArray[Long] = null
  var eids : Array[Long] = null
  if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
    val projectedFragment = fragment.asInstanceOf[ArrowProjectedAdaptor[Long,Long,_,_]].asInstanceOf[ArrowProjectedFragment[Long,Long,_,_]]
    oePtr = projectedFragment.getOutEdgesPtr
    iePtr = projectedFragment.getInEdgesPtr
    oePtrStartAddr = oePtr.getAddress
    iePtrStartAddr = iePtr.getAddress
    oeOffsetBeginArray = projectedFragment.getOEOffsetsBeginAccessor.asInstanceOf[TypedArray[Long]]
    ieOffsetBeginArray = projectedFragment.getIEOffsetsBeginAccessor.asInstanceOf[TypedArray[Long]]
    oeOffsetEndArray = projectedFragment.getOEOffsetsEndAccessor.asInstanceOf[TypedArray[Long]]
    ieOffsetEndArray = projectedFragment.getIEOffsetsEndAccessor.asInstanceOf[TypedArray[Long]]
    eids = {
      val edgeNum = getOutEdgesNum
      val res = new Array[Long](edgeNum.toInt)
      val nbr = projectedFragment.getOutEdgesPtr
      var i = 0
      while (i < edgeNum){
        res(i) = nbr.eid()
        nbr.addV(16)
        i += 1
      }
      res
    }

  }
  else {
    throw new IllegalStateException(s"not supported type ${fragment.fragmentType()}")
  }

  val lid2Oid : Array[Long] = {
    val res = new Array[Long](fragment.getVerticesNum.toInt)
    var i = 0L
    val limit = res.length
    while (i < limit){
      vertex.SetValue(i)
      res(i.toInt) = fragment.getId(vertex)
      i += 1
    }
    res
  }

  val startLid = 0
  val endLid: Long = fragment.getInnerVerticesNum()
  //FIXME: implement these
//  override val inDegreeArray: Array[Int] = getInDegreeArray
//  override val outDegreeArray: Array[Int] = getOutDegreeArray
//  override val inOutDegreeArray: Array[Int] = getInOutDegreeArray
  override val mirrorVertices: Array[BitSet] = getMirrorVertices

  @inline
  override def getOEBeginOffset(lid : Long) : Long = {
    oeOffsetBeginArray.get(lid)
  }

  @inline
  override def getIEBeginOffset(lid : Long) : Long = {
    ieOffsetBeginArray.get(lid)
  }

  @inline
  override def getOEEndOffset(lid : Long) : Long = {
    oeOffsetEndArray.get(lid + 1)
  }

  @inline
  override def getIEEndOffset(lid : Long) : Long = {
    ieOffsetEndArray.get(lid + 1)
  }

  private def getMirrorVertices : Array[BitSet] = {
    if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
      val projectedFragment = fragment.asInstanceOf[ArrowProjectedAdaptor[Long, Long, _, _]].asInstanceOf[ArrowProjectedFragment[Long, Long, _, _]]

      val res = new Array[BitSet](fnum())
      for (i <- 0 until fnum()) {
        val vec = projectedFragment.mirrorVertices(i)
        val size = vec.size().toInt
        log.info(s"frag ${fid()} has ${size} mirror vertices on frag-${i}")
        val curArray = new BitSet(size)
        var j =0
        while (j < size){
          curArray.set(vec.get(j).GetValue().toInt)
          j += 1
        }
        res(i) = curArray
      }
      res
    }
    else {
      throw  new IllegalStateException("Not supported")
    }
  }

  private val lid2Offset : Array[Long] = {
    val res = new Array[Long](endLid.toInt + 1)
    res(0) = 0
    for (i <- 0 until endLid.toInt){
      res(i + 1) = res(i) + getOutDegree(i)
    }
    log.info("initialize lid2offset")
    res
  }

  def outDegreeArray(startLid : Long, endLid : Long) : Array[Int] = {
    val time0 = System.nanoTime()
    val len = fragment.getVerticesNum.toInt
    val res = new Array[Int](len)
    var i = startLid.toInt
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

  def inDegreeArray(startLid : Long, endLid : Long) : Array[Int] = {
    val time0 = System.nanoTime()
    val len = fragment.getVerticesNum.toInt
    val res = new Array[Int](len)
    var i = startLid.toInt
    while (i < endLid){
      res(i) = getInDegree(i).toInt
      i += 1
    }
    while (i < len){
      res(i) = 0
      i += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Get in degree array cost ${(time1 - time0)/1000000} ms")
    res
  }

  def inOutDegreeArray(startLid : Long, endLid : Long) : Array[Int] = {
    val len = fragment.getVerticesNum.toInt
    val res = new Array[Int](len)
    var i = 0
    while (i < endLid) {
      res(i) = getInDegree(i).toInt + getOutDegree(i).toInt
      i += 1
    }
    while (i < len) {
      res(i) =  0
      i += 1
    }
    res
  }

  override def getInDegree(vid: Long): Long = {
    vertex.SetValue(vid)
    fragment.getLocalInDegree(vertex)
  }

  override def getOutDegree(vid: Long): Long = {
    vertex.SetValue(vid)
    fragment.getLocalOutDegree(vertex)
  }

  override def isInEdgesEmpty(vid: Long): Boolean = {
    vertex.SetValue(vid)
    fragment.getLocalInDegree(vertex) == 0
  }

  override def isOutEdgesEmpty(vid: Long): Boolean = {
    vertex.SetValue(vid)
    fragment.getLocalOutDegree(vertex) == 0
  }

  override def getInEdgesNum: Long = fragment.getInEdgeNum

  override def getOutEdgesNum: Long = fragment.getOutEdgeNum


  override def fid(): Int = fragment.fid()

  override def fnum(): Int = fragment.fnum()

  override def getId(vid: Long): Long = {
    vertex.SetValue(vid)
    fragment.getId(vertex)
  }

  override def getVertex(oid: Long, vertex: Vertex[Long]): Boolean = {
    fragment.getVertex(oid, vertex)
  }

  override def getTotalVertexSize: Long = fragment.getTotalVerticesNum

  override def getVertexSize: Long = fragment.getVerticesNum

  override def getInnerVertexSize: Long = fragment.getInnerVerticesNum

  override def innerVertexLid2Oid(lid: Long): Long = {
    vertex.SetValue(lid)
    fragment.getInnerVertexId(vertex);
  }

  override def outerVertexLid2Oid(lid: Long): Long = {
    vertex.SetValue(lid)
    fragment.getOuterVertexId(vertex)
  }

  override def getOuterVertexSize: Long = fragment.getOuterVerticesNum

  override def innerOid2Gid(oid: Long): Long = {
    fragment.getInnerVertex(oid, vertex)
    fragment.getInnerVertexGid(vertex)
  }

  override def getOuterVertexGid(lid: Long): Long = {
    vertex.SetValue(lid)
    fragment.getOuterVertexGid(vertex)
  }

  def initFid2GraphxPid(array: Array[(PartitionID, Int)]) : Unit = {
    for (tuple <- array){
      fid2Pid(tuple._2) = tuple._1
    }
    log.info(s"Filled fid2 graphx pid ${fid2Pid.mkString("Array(", ", ", ")")}")
  }

  override def fid2GraphxPid(fid: Int): Int = {
    fid2Pid(fid)
  }

  override def outerVertexGid2Vertex(gid: Long, vertex: Vertex[Long]): Boolean = {
    fragment.outerVertexGid2Vertex(gid, vertex)
  }

  /** For us, the input edatas should be null, and we shall not reply on it to get edge data. */
  override def iterator[ED: ClassTag](startLid : Long, endLid : Long, edatas: ArrayWithOffset[ED], activeSet: BitSetWithOffset, edgeReversed: Boolean): Iterator[Edge[ED]] = {
    if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)){
      val projectedFragment = fragment.asInstanceOf[ArrowProjectedAdaptor[Long,Long,_,ED]]
      newProjectedIterator(startLid, endLid, projectedFragment.getArrowProjectedFragment.asInstanceOf[ArrowProjectedFragment[Long,Long,_,ED]],edatas,activeSet,edgeReversed)
    }
    else {
      throw new IllegalStateException("Not implemented")
    }
  }

  override def tripletIterator[VD: ClassTag, ED: ClassTag](startLid : Long, endLid : Long,innerVertexDataStore: VertexDataStore[VD],outerVertexDataStore: VertexDataStore[VD], edatas: ArrayWithOffset[ED], activeSet: BitSetWithOffset,edgeReversed : Boolean = false, includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false, includeLid : Boolean = false): Iterator[EdgeTriplet[VD, ED]] = {
    if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)){
      val projectedFragment = fragment.asInstanceOf[ArrowProjectedAdaptor[Long,Long,VD,ED]].getArrowProjectedFragment.asInstanceOf[ArrowProjectedFragment[Long,Long,VD,ED]]
      log.info(s"creating triplet iterator v2 with java edata, with inner vd store ${innerVertexDataStore} outer vd store ${outerVertexDataStore}")
      newProjectedTripletIterator(startLid,endLid,projectedFragment, innerVertexDataStore,outerVertexDataStore,edatas,activeSet,edgeReversed,includeSrc,includeDst,includeLid)
    }
    else {
      throw new IllegalStateException("Not implemented")
    }
  }

  private def newProjectedIterator[ED : ClassTag](startLid : Long, endLid : Long,frag: ArrowProjectedFragment[Long, Long, _, ED], edatas : ArrayWithOffset[ED], activeEdgeSet: BitSetWithOffset, edgeReverse : Boolean) : Iterator[Edge[ED]] = {
    if (edgeReverse){
      new Iterator[Edge[ED]] {
        var curOffset = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
        val edge = new ReusableEdgeImpl[ED]
        var curLid = startLid.toInt
        val beginAddr = frag.getOutEdgesPtr.getAddress
        val nbr = frag.getOutEdgesPtr
        var curEndOffset = oeOffsetEndArray.get(curLid)
        edge.dstId = lid2Oid(curLid)
        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            if (curOffset < 0) return false
            while (curOffset >= curEndOffset && curLid < endLid) {
              curEndOffset = oeOffsetEndArray.get(curLid + 1)
              curLid += 1
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
          curOffset = activeEdgeSet.nextSetBit(curOffset)
          edge
        }
      }
    }
    else {
      new Iterator[Edge[ED]] {
        var curOffset = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
        val edge = new ReusableEdgeImpl[ED]
        var curLid = startLid.toInt
        val beginAddr = frag.getOutEdgesPtr.getAddress
        val nbr = frag.getOutEdgesPtr
        var curEndOffset = oeOffsetEndArray.get(curLid)
        edge.srcId = lid2Oid(curLid)
        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            if (curOffset < 0) return false
            while (curOffset >= curEndOffset && curLid < endLid) {
              curEndOffset = oeOffsetEndArray.get(curLid + 1)
              curLid += 1
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
          curOffset = activeEdgeSet.nextSetBit(curOffset)
          edge
        }
      }
    }

  }
  private def newProjectedTripletIterator[VD: ClassTag,ED : ClassTag](startLid : Long, endLid : Long,frag: ArrowProjectedFragment[Long, Long, VD, ED],innerVertexDataStore: VertexDataStore[VD],outerVertexDataStore: VertexDataStore[VD], edatas : ArrayWithOffset[ED],activeEdgeSet: BitSetWithOffset, edgeReversed: Boolean, includeSrc : Boolean, includeDst : Boolean,includeLid : Boolean = false) : Iterator[EdgeTriplet[VD,ED]] = {
    if (!edgeReversed){
      new Iterator[EdgeTriplet[VD, ED]] {
        var curOffset = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
        var curLid = startLid.toInt
        var srcId = lid2Oid(curLid)
        var srcAttr = innerVertexDataStore.getData(curLid)
        val beginAddr = frag.getOutEdgesPtr.getAddress
        val nbr = frag.getOutEdgesPtr
        var curEndOffset = oeOffsetEndArray.get(curLid)

        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            if (curOffset < 0) return false
            while (curOffset >= curEndOffset && curLid < endLid) {
              curEndOffset = oeOffsetEndArray.get(curLid + 1)
              curLid += 1
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
            if (dstLid >= ivnum){
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
        var curLid = startLid.toInt
        var dstId = lid2Oid(curLid)
        var dstAttr = innerVertexDataStore.getData(curLid)
        val beginAddr = frag.getOutEdgesPtr.getAddress
        val nbr = frag.getOutEdgesPtr
        var curEndOffset = oeOffsetEndArray.get(curLid)

        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            if (curOffset < 0) return false
            while (curOffset >= curEndOffset && curLid < endLid) {
              curEndOffset = oeOffsetEndArray.get(curLid + 1)
              curLid += 1
            }
            if (curLid >= endLid) return false
            dstId = lid2Oid(curLid)
            dstAttr = innerVertexDataStore.getData(curLid)
            true
          }
        }

        override def next(): EdgeTriplet[VD, ED] = {
          val edgeTriplet = new GSEdgeTripletImpl[VD, ED]
          nbr.setAddress(beginAddr + curOffset * 16)
          val srcLid = nbr.vid().toInt
          edgeTriplet.eid = nbr.eid()
          edgeTriplet.offset = curOffset
          edgeTriplet.srcId = lid2Oid(srcLid)
          edgeTriplet.srcAttr = {
            if (srcLid >= ivnum) outerVertexDataStore.getData(srcLid)
            innerVertexDataStore.getData(srcLid)
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
    require(fragment.getInnerVertex(oid,vertex))
    true
  }

  override val structureType: GraphStructureType = ArrowProjectedStructure

  override def getEids: Array[VertexId] = eids

  override def getOutNbrIds(vid: VertexId): Array[VertexId] = {
    val size = getOutDegree(vid)
    val res = new Array[VertexId](size.toInt)
    fillOutNbrIdsImpl(vid, res)
    res
  }

  def fillOutNbrIdsImpl(vid : VertexId, array : Array[VertexId], startInd : Int = 0) : Unit = {
    var curOff = oeOffsetBeginArray.get(vid)
    val endOff = oeOffsetEndArray.get(vid)
    oePtr.setAddress(oePtrStartAddr + curOff * 16)
    var i = startInd
    while (curOff < endOff){
      val dstOid = getId(oePtr.vid())
      array(i) = dstOid
      curOff += 1
      i += 1
      oePtr.addV(16)
    }
  }

  override def getInNbrIds(vid: VertexId): Array[VertexId] = {
    val size = getInDegree(vid)
    val res = new Array[VertexId](size.toInt)
    fillInNbrIdsImpl(vid, res)
    res
  }

  def fillInNbrIdsImpl(vid : VertexId, array : Array[VertexId], startInd : Int = 0) : Unit = {
    var curOff = ieOffsetBeginArray.get(vid)
    val endOff = ieOffsetEndArray.get(vid)
    iePtr.setAddress(iePtrStartAddr + curOff * 16)
    var i = startInd
    while (curOff < endOff){
      val dstOid = getId(iePtr.vid())
      array(i) = dstOid
      curOff += 1
      i += 1
      iePtr.addV(16)
    }
  }

  override def getInOutNbrIds(vid: VertexId): Array[VertexId] = {
    val size = getInDegree(vid) + getOutDegree(vid)
    val res = new Array[VertexId](size.toInt)
    fillOutNbrIdsImpl(vid, res, 0)
    fillInNbrIdsImpl(vid, res, getOutDegree(vid).toInt)
    res
  }

  //FIXME: implement this.
  override def iterateTriplets[VD: ClassTag, ED: ClassTag,ED2: ClassTag](startLid : Long, endLid : Long,f: EdgeTriplet[VD,ED] => ED2, innerVertexDataStore: VertexDataStore[VD],outerVertexDataStore: VertexDataStore[VD], edatas: ArrayWithOffset[ED], activeSet: BitSetWithOffset, edgeReversed: Boolean, includeSrc: Boolean, includeDst: Boolean, newArray : ArrayWithOffset[ED2]): Unit = {
    throw new IllegalStateException("Not implemented")
  }

  override def iterateEdges[ED: ClassTag, ED2: ClassTag](startLid : Long, endLid : Long,f: Edge[ED] => ED2, edatas: ArrayWithOffset[ED], activeSet: BitSetWithOffset, edgeReversed: Boolean, newArray: ArrayWithOffset[ED2]): Unit = {
    throw new IllegalStateException("Not implemented")
  }

  override def getOEOffsetRange(startLid: VertexId, endLid: VertexId): (VertexId, VertexId) = {
    (oeOffsetBeginArray.get(startLid), oeOffsetEndArray.get(endLid - 1))
  }
}

object FragmentStructure{
  val NBR_SIZE = 16
}
