package com.alibaba.graphscope.graphx.graph.impl

import com.alibaba.graphscope.ds.{PropertyNbrUnit, TypedArray, Vertex}
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor
import com.alibaba.graphscope.fragment.{ArrowProjectedFragment, FragmentType, IFragment}
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.{ArrowProjectedStructure, GraphStructureType}
import com.alibaba.graphscope.graphx.graph.impl.FragmentStructure.NBR_SIZE
import com.alibaba.graphscope.graphx.graph.{GSEdgeTripletImpl, GraphStructure, ReusableEdge, ReusableEdgeImpl}
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.partition.data.VertexDataStore
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

class FragmentStructure(val fragment : IFragment[Long,Long,_,_],
                        var srcLids : PrimitiveArray[Long] = null,var dstLids : PrimitiveArray[Long] = null,
                        var srcOids : PrimitiveArray[Long] = null, var dstOids : PrimitiveArray[Long] = null,
                        var eids : PrimitiveArray[Long] = null) extends GraphStructure with Logging{
  val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
  val fid2Pid = new Array[Int](fragment.fnum())


  var oePtr,iePtr : PropertyNbrUnit[Long] = null
  var oePtrStartAddr,iePtrStartAddr : Long = 0
  var oeOffsetBeginArray,ieOffsetBeginArray : TypedArray[Long] = null
  var oeOffsetEndArray,ieOffsetEndArray : TypedArray[Long] = null
  if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
    val projectedFragment = fragment.asInstanceOf[ArrowProjectedAdaptor[Long,Long,_,_]].asInstanceOf[ArrowProjectedFragment[Long,Long,_,_]]
    oePtr = projectedFragment.getOutEdgesPtr
    iePtr =projectedFragment.getInEdgesPtr
    oePtrStartAddr = oePtr.getAddress
    iePtrStartAddr = iePtr.getAddress
    oeOffsetBeginArray = projectedFragment.getOEOffsetsBeginAccessor.asInstanceOf[TypedArray[Long]]
    ieOffsetBeginArray = projectedFragment.getIEOffsetsBeginAccessor.asInstanceOf[TypedArray[Long]]
    oeOffsetEndArray = projectedFragment.getOEOffsetsEndAccessor.asInstanceOf[TypedArray[Long]]
    ieOffsetEndArray = projectedFragment.getIEOffsetsEndAccessor.asInstanceOf[TypedArray[Long]]

  }
  else {
    throw new IllegalStateException(s"not supported type ${fragment.fragmentType()}")
  }

  val startLid = 0
  val endLid: Long = fragment.getInnerVerticesNum()
  //FIXME: implement these
  override val inDegreeArray: PrimitiveArray[Int] = getInDegreeArray
  override val outDegreeArray: PrimitiveArray[Int] = getOutDegreeArray
  override val inOutDegreeArray: PrimitiveArray[Int] = getInOutDegreeArray

  private val lid2Offset : Array[Long] = {
    val res = new Array[Long](endLid.toInt + 1)
    res(0) = 0
    for (i <- 0 until endLid.toInt){
      res(i + 1) = res(i) + getOutDegree(i)
    }
    log.info("initialize lid2offset")
    res
  }

  if (srcLids == null){
    require(dstLids == null && srcOids == null && dstOids == null)
    if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
      val projectedFragment = fragment.asInstanceOf[ArrowProjectedAdaptor[Long, Long, _, _]].getArrowProjectedFragment.asInstanceOf[ArrowProjectedFragment[Long,Long,_,_]]
      srcOids = PrimitiveArray.create(classOf[Long], getOutEdgesNum.toInt)
      srcLids = PrimitiveArray.create(classOf[Long], getOutEdgesNum.toInt)
      dstOids = PrimitiveArray.create(classOf[Long], getOutEdgesNum.toInt)
      dstLids = PrimitiveArray.create(classOf[Long], getOutEdgesNum.toInt)
      eids = PrimitiveArray.create(classOf[Long], getOutEdgesNum.toInt)
      val nbr = projectedFragment.getOutEdgesPtr

      val time0 = System.nanoTime()
      var curLid = 0
      while (curLid < endLid){
        vertex.SetValue(curLid)
        val curOid = fragment.getId(vertex)
        val startOffset = lid2Offset(curLid)
        val endOffset = lid2Offset(curLid + 1)
        var tmp = startOffset
        while (tmp < endOffset){
          srcLids.set(tmp, curLid)
          srcOids.set(tmp, curOid)
          tmp += 1
        }
        tmp = startOffset
        while (tmp < endOffset){
          val dstLid = nbr.vid()
          vertex.SetValue(dstLid)
          val dstOid = fragment.getId(vertex)
          dstLids.set(tmp, dstLid)
          dstOids.set(tmp, dstOid)
          eids.set(tmp, nbr.eid())
          nbr.addV(NBR_SIZE)
          tmp += 1
        }
        curLid += 1
      }
      val time1 = System.nanoTime()
      log.info(s"initialize arrays cost ${(time1 - time0)/1000000} ms")
    }
    else {
      throw new IllegalStateException(s"No implementation for ${fragment.fragmentType()}")
    }
  }

  private def getOutDegreeArray : PrimitiveArray[Int] = {
    val time0 = System.nanoTime()
    val len = fragment.getVerticesNum.toInt
    val res = PrimitiveArray.create(classOf[Int], len)
    var i = 0L
    while (i < endLid){
      res.set(i, getOutDegree(i).toInt)
      i += 1
    }
    while (i < len){
      res.set(i, 0)
      i += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Get out degree array cost ${(time1 - time0)/1000000} ms")
    res
  }

  private def getInDegreeArray : PrimitiveArray[Int] = {
    val time0 = System.nanoTime()
    val len = fragment.getVerticesNum.toInt
    val res = PrimitiveArray.create(classOf[Int], len)
    var i = 0L
    while (i < endLid){
      res.set(i, getInDegree(i).toInt)
      i += 1
    }
    while (i < len){
      res.set(i, 0)
      i += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Get in degree array cost ${(time1 - time0)/1000000} ms")
    res
  }

  private def getInOutDegreeArray : PrimitiveArray[Int] = {
    val len = fragment.getVerticesNum.toInt
    val res = PrimitiveArray.create(classOf[Int], len)
    var i = 0L
    while (i < endLid) {
      res.set(i, getInDegree(i).toInt + getOutDegree(i).toInt)
      i += 1
    }
    while (i < len) {
      res.set(i, 0)
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
  override def iterator[ED: ClassTag](edatas: PrimitiveArray[ED], activeSet: BitSet, edgeReversed: Boolean): Iterator[Edge[ED]] = {
    if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)){
      val projectedFragment = fragment.asInstanceOf[ArrowProjectedAdaptor[Long,Long,_,ED]]
      newProjectedIterator(projectedFragment.getArrowProjectedFragment.asInstanceOf[ArrowProjectedFragment[Long,Long,_,ED]],edatas,activeSet,edgeReversed)
    }
    else {
      throw new IllegalStateException("Not implemented")
    }
  }

  override def tripletIterator[VD: ClassTag, ED: ClassTag](vertexDataStore: VertexDataStore[VD], edatas: PrimitiveArray[ED], activeSet: BitSet,edgeReversed : Boolean = false, includeSrc: Boolean = true, includeDst: Boolean = true): Iterator[EdgeTriplet[VD, ED]] = {
    if (fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)){
      val projectedFragment = fragment.asInstanceOf[ArrowProjectedAdaptor[Long,Long,VD,ED]].getArrowProjectedFragment.asInstanceOf[ArrowProjectedFragment[Long,Long,VD,ED]]
      log.info(s"creating triplet iterator v2 with java edata, with vd store ${vertexDataStore}")
      newProjectedTripletIterator(projectedFragment, vertexDataStore,edatas,activeSet,edgeReversed,includeSrc,includeDst)
    }
    else {
      throw new IllegalStateException("Not implemented")
    }
  }

  private def newProjectedIterator[ED : ClassTag](frag: ArrowProjectedFragment[Long, Long, _, ED], edatas : PrimitiveArray[ED], bitSet: BitSet, edgeReverse : Boolean) : Iterator[Edge[ED]] = {
    if (edgeReverse){
      new Iterator[Edge[ED]]{
        val edge: ReusableEdge[ED] =  new ReusableEdgeImpl[ED];
        var offset : Long = bitSet.nextSetBit(0)
        val nbr : PropertyNbrUnit[Long]= frag.getOutEdgesPtr
        val initAddress : Long = nbr.getAddress

        override def hasNext: Boolean = {
          offset >= 0
        }

        override def next(): Edge[ED] = {
          nbr.setAddress(initAddress + NBR_SIZE * offset)
          edge.srcId = dstOids.get(offset)
          edge.dstId = srcOids.get(offset)
          edge.eid = eids.get(offset)
          edge.offset = offset
          edge.attr = edatas.get(edge.eid)
          offset = bitSet.nextSetBit((offset + 1).toInt)
          edge
        }
      }
    }
    else {
      new Iterator[Edge[ED]]{
        val edge: ReusableEdge[ED] =  new ReusableEdgeImpl[ED];
        var offset : Long = bitSet.nextSetBit(0)
        val nbr : PropertyNbrUnit[Long]= frag.getOutEdgesPtr
        val initAddress : Long = nbr.getAddress

        override def hasNext: Boolean = {
          offset >= 0
        }

        override def next(): Edge[ED] = {
          nbr.setAddress(initAddress + NBR_SIZE * offset)
          edge.dstId = dstOids.get(offset)
          edge.srcId = srcOids.get(offset)
          edge.eid = eids.get(offset)
          edge.offset = offset
          edge.attr = edatas.get(edge.eid)
          offset = bitSet.nextSetBit((offset + 1).toInt)
          edge
        }
      }
    }

  }
  private def newProjectedTripletIterator[VD: ClassTag,ED : ClassTag](frag: ArrowProjectedFragment[Long, Long, VD, ED],vertexDataStore: VertexDataStore[VD], edatas : PrimitiveArray[ED],bitSet: BitSet, edgeReversed: Boolean, includeSrc : Boolean, includeDst : Boolean) : Iterator[EdgeTriplet[VD,ED]] = {
    if (edgeReversed){
      new Iterator[EdgeTriplet[VD,ED]]{
        var offset : Long = bitSet.nextSetBit(0)
        override def hasNext: Boolean = {
          offset >= 0
        }

        override def next(): EdgeTriplet[VD,ED] = {
          val edge = new GSEdgeTripletImpl[VD,ED]
          edge.dstId = srcOids.get(offset)
          if (includeDst){
            edge.dstAttr = vertexDataStore.getData(srcLids.get(offset))
          }
          edge.srcId = dstOids.get(offset)
          if (includeSrc){
            edge.srcAttr = vertexDataStore.getData(dstLids.get(offset))
          }
          edge.eid = eids.get(offset)
          edge.attr = edatas.get(edge.eid)
          edge.offset = offset
          offset = bitSet.nextSetBit((offset + 1).toInt)
          edge
        }
      }
    }
    else {
      new Iterator[EdgeTriplet[VD, ED]] {
        var offset: Long = bitSet.nextSetBit(0)

        override def hasNext: Boolean = {
          offset >= 0
        }

        override def next(): EdgeTriplet[VD, ED] = {
          val edge = new GSEdgeTripletImpl[VD, ED]
          edge.srcId = srcOids.get(offset)
          if (includeSrc) {
            edge.srcAttr = vertexDataStore.getData(srcLids.get(offset))
          }
          edge.dstId = dstOids.get(offset)
          if (includeDst) {
            edge.dstAttr = vertexDataStore.getData(dstLids.get(offset))
          }
          edge.eid = eids.get(offset)
          edge.attr = edatas.get(edge.eid)
          edge.offset = offset
          offset = bitSet.nextSetBit((offset + 1).toInt)
          edge
        }
      }
    }
  }

  override def getInnerVertex(oid: Long, vertex: Vertex[Long]): Boolean = {
    require(fragment.getInnerVertex(oid,vertex))
    true
  }

  override val structureType: GraphStructureType = ArrowProjectedStructure

  override def getEids: PrimitiveArray[VertexId] = eids

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
}

object FragmentStructure{
  val NBR_SIZE = 16
}
