package com.alibaba.graphscope.graphx.graph.impl

import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime
import com.alibaba.graphscope.ds.{ImmutableTypedArray, Vertex}
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.{GraphStructureType, GraphXFragmentStructure}
import com.alibaba.graphscope.graphx.graph.{GSEdgeTripletImpl, GraphStructure, ReusableEdgeImpl}
import com.alibaba.graphscope.graphx.store.{AbstractDataStore, DataStore, AbstractInHeapDataStore}
import com.alibaba.graphscope.graphx.utils.{ArrayWithOffset, BitSetWithOffset, IdParser, PrimitiveVector}
import com.alibaba.graphscope.utils.ThreadSafeBitSet
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/** the edge array only contains out edges, we use in edge as a comparison  */
class GraphXGraphStructure(val vm : GraphXVertexMap[Long,Long], val csr : GraphXCSR[Long], val parallelism : Int) extends GraphStructure with Logging{
  val oeBeginNbr = csr.getOEBegin(0)
  val oeBeginAddr = oeBeginNbr.getAddress
  val oeEndAddr = csr.getOEEnd(vm.innerVertexSize() - 1).getAddress
  val ieBeginNbr = csr.getIEBegin(0)
  val ieBeginAddr = ieBeginNbr.getAddress
  val ivnum = vm.innerVertexSize()
  val tvnum = vm.getVertexSize.toInt
  val lid2Oid: Array[ImmutableTypedArray[VertexId]] = {
    val res = new Array[ImmutableTypedArray[Long]](vm.fnum())
    for (i <- 0 until fnum()){
      res(i) = vm.getLid2OidAccessor(i)
    }
    res
  }
  val idParser = new IdParser(fnum())
  val outerLid2Gid: ImmutableTypedArray[VertexId] = vm.getOuterLid2GidAccessor
  require(outerLid2Gid.getLength == (tvnum - ivnum), s"ovnum neq ${outerLid2Gid.getLength} vs ${tvnum - ivnum}")

  val myFid: Int = vm.fid()

//  lazy val inDegreeArray: Array[Int] = getInDegreeArray
//  lazy val outDegreeArray: Array[Int] = getOutDegreeArray
//  lazy val inOutDegreeArray: Array[Int] = getInOutDegreeArray
  //FIXME: bitset for long


  val oeOffsetsArray: ImmutableTypedArray[Long] = csr.getOEOffsetsArray.asInstanceOf[ImmutableTypedArray[Long]]
  val oeOffsetsLen : Long = oeOffsetsArray.getLength

  val ieOffsetsArray : ImmutableTypedArray[Long] = csr.getIEOffsetsArray.asInstanceOf[ImmutableTypedArray[Long]]

  override val mirrorVertices: Array[ThreadSafeBitSet] = getMirrorVertices
//  val dstOids : Array[Long] = new Array[Long](csr.getOutEdgesNum.toInt)
//  val dstLids : Array[Int] = new Array[Int](csr.getOutEdgesNum.toInt)
//  def init() = {
//    val time0 = System.nanoTime()
//    val nbr = csr.getOEBegin(0)
//    var offset = 0
//    val limit = csr.getOEOffset(ivnum)
//    while (offset < limit){
//      val dstLid = nbr.vid().toInt
//      dstOids(offset) = getId(dstLid)
//      dstLids(offset) = dstLid
//      offset += 1
//      nbr.addV(16)
//    }
//    val time1 = System.nanoTime()
//    log.info(s"[Init cost ${(time1 - time0)/1000000} ms]")
//  }
//  init()


  @inline
  override def getOEBeginOffset(lid : Int) : Long = {
    require(lid < oeOffsetsLen, s"index out of range ${lid}, ${oeOffsetsLen}")
    oeOffsetsArray.get(lid)
  }

  @inline
  override def getIEBeginOffset(lid : Int) : Long = {
    ieOffsetsArray.get(lid)
  }

  @inline
  override def getOEEndOffset(lid : Int) : Long = {
    require(lid + 1 < oeOffsetsLen, s"index out of range ${lid}, ${oeOffsetsLen}")
    oeOffsetsArray.get(lid + 1)
  }

  @inline
  override def getIEEndOffset(lid : Int) : Long = {
    ieOffsetsArray.get(lid + 1)
  }

  @inline
  def getOutDegree(l: Int) : Long = {
    oeOffsetsArray.get(l + 1) - oeOffsetsArray.get(l)
  }

  @inline
  def getInDegree(l: Int) : Long = {
    ieOffsetsArray.get(l + 1) - ieOffsetsArray.get(l)
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

  private def getMirrorVertices : Array[ThreadSafeBitSet] = {
    val time0 = System.nanoTime()
    val res = new Array[ThreadSafeBitSet](fnum())
    val ivnum = vm.innerVertexSize().toInt
    for (i <- res.indices){
      res(i) = new ThreadSafeBitSet()
    }
    val curFid = fid()
    val threads = new Array[Thread](parallelism)
    val atomicInt = new AtomicInteger(0)
    val batchSize = 4096
    var tid = 0
    while (tid < parallelism){
      val newThread= new Thread() {
        override def run(): Unit = {
          var flag = true
          val flags = new Array[Boolean](fnum())
          while (flag) {
            val begin = Math.min(atomicInt.getAndAdd(batchSize), ivnum)
            val end = Math.min(begin + batchSize, ivnum)
            if (begin >= end) {
              flag = false
            }
            else {
              var lid = begin
              while (lid < end) {
                for (i <- flags.indices) {
                  flags(i) = false
                }
                //only in edges are enough
                var beginOffset = getIEBeginOffset(lid)
                val endOffset = getIEEndOffset(lid)
                var address = ieBeginAddr + (beginOffset << 4)
                while (beginOffset < endOffset) {
                  val dstLid = JavaRuntime.getLong(address)
                  if (dstLid >= ivnum) {
                    val dstFid = getOuterVertexFid(dstLid)
                    flags(dstFid) = true
                  }
                  address += 16
                  beginOffset += 1
                }
                var oeBeginOffset = getOEBeginOffset(lid)
                val oeEndOffset = getOEEndOffset(lid)
                address = oeBeginAddr + (oeBeginOffset << 4)
                while (oeBeginOffset < oeEndOffset) {
                  val dstLid = JavaRuntime.getLong(address)
                  if (dstLid >= ivnum) {
                    val dstFid = getOuterVertexFid(dstLid)
                    flags(dstFid) = true
                  }
                  address += 16
                  oeBeginOffset += 1
                }
                for (i <- flags.indices) {
                  if (flags(i) && i != curFid) {
                    res(i).set(lid)
                  }
                }
                lid += 1
              }
            }
          }
        }
      }
      newThread.start()
      threads(tid) = newThread
      tid += 1
    }
    for (i <- 0 until parallelism){
      threads(i).join()
    }

    val time1 = System.nanoTime()
    log.info(s"[Got mirror vertices cost ${(time1 - time0)/1000000} ms]")
    res
  }

  @inline
  def getOuterVertexFid(lid: Long) : Int = {
    val gid = outerLid2Gid.get(lid - ivnum)
    idParser.getFragId(gid)
  }

//  override def getInDegree(vid: Long): Long = csr.getInDegree(vid)
//
//  override def getOutDegree(vid: Long): Long = csr.getOutDegree(vid)

  override def isInEdgesEmpty(vid: Long): Boolean = csr.isInEdgesEmpty(vid)

  override def isOutEdgesEmpty(vid: Long): Boolean = csr.isOutEdgesEmpty(vid)

  override def getInEdgesNum: Long = csr.getInEdgesNum

  override def getOutEdgesNum: Long = csr.getOutEdgesNum

  override def getTotalEdgesNum: VertexId = csr.getTotalEdgesNum

  override def fid(): Int = myFid

  override def fnum(): Int = vm.fnum()

  @noinline
  override def getId(vertex: Long): Long = {
    if (vertex < ivnum){
      innerVertexLid2Oid(vertex)
    }
    else if (vertex < tvnum){
      outerVertexLid2Oid(vertex)
    }
    else {
      throw new IllegalStateException(s"not possible ${vertex}")
    }
  }

  override def getVertex(oid: Long, vertex: Vertex[Long]): Boolean = vm.getVertex(oid,vertex)

  override def getTotalVertexSize: Long = vm.getTotalVertexSize

  override def getVertexSize: Long = vm.getVertexSize

  override def getInnerVertexSize: Long = vm.innerVertexSize()

  @noinline
  override def innerVertexLid2Oid(lid: Long): Long = {
    require(lid < ivnum, s"index out of range ${lid}, ${ivnum}")
    lid2Oid(myFid).get(lid)
  }

  @noinline
  override def outerVertexLid2Oid(vertex: Long): Long = {
    require(vertex >= ivnum && vertex < tvnum, s"index out of range ${vertex}, ${ivnum} ~ ${tvnum}")
    val gid = outerLid2Gid.get(vertex - ivnum)
    val lid = idParser.getLocalId(gid)
    val fid = idParser.getFragId(gid)
    require(fid != myFid, s"outer fid can not equal to me ${fid}, ${myFid}, gid ${gid}")
    require(fid < fnum(), s"fid greater than fnum ${fid}, ${fnum()}")
    val len = lid2Oid(fid).getLength
    require(lid < len, s"lid should less than ivnum of frag ${fid}, lid ${lid}, len ${len}")
    lid2Oid(fid).get(lid)
  }

  override def getOuterVertexSize: Long = vm.getOuterVertexSize

  override def innerOid2Gid(oid: Long): Long = vm.innerOid2Gid(oid)

  override def getOuterVertexGid(lid: Long): Long = vm.getOuterVertexGid(lid)

  override def fid2GraphxPid(fid: Int): Int = vm.fid2GraphxPid(fid)

  //FIXME: accelerate this.
  override def outerVertexGid2Vertex(gid: Long, vertex: Vertex[Long]): Boolean = vm.outerVertexGid2Vertex(gid,vertex)

  override def iterator[ED: ClassTag](startLid : Long, endLid : Long, edatas: DataStore[ED], activeEdgeSet : BitSetWithOffset, edgeReversed : Boolean = false): Iterator[Edge[ED]] = {
    if (!edgeReversed){
      new Iterator[Edge[ED]] {
        var curOffset = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
        val edge = new ReusableEdgeImpl[ED]
        var curLid = startLid.toInt
        var curEndOffset = getOEEndOffset(curLid)
        edge.srcId = innerVertexLid2Oid(curLid)
        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            if (curOffset < 0) return false
            while (curOffset >= curEndOffset && curLid < endLid) {
              curLid += 1
              curEndOffset = getOEEndOffset(curLid)
            }
            if (curLid >= endLid) return false
            edge.srcId = innerVertexLid2Oid(curLid)
            true
          }
        }

        override def next(): Edge[ED] = {
          edge.dstId = getId(JavaRuntime.getLong(oeBeginAddr + (curOffset << 4)))
          edge.attr = edatas.getData(curOffset)
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
        var curEndOffset = getOEEndOffset(curLid)
        edge.dstId = innerVertexLid2Oid(curLid)
        override def hasNext: Boolean = {
          if (curOffset < curEndOffset && curOffset >= 0) true
          else {
            if (curOffset < 0) return false
            while (curOffset >= curEndOffset && curLid < endLid) {
              curLid += 1
              curEndOffset = getOEEndOffset(curLid)
            }
            if (curLid >= endLid) return false
            edge.dstId = innerVertexLid2Oid(curLid)
            true
          }
        }

        override def next(): Edge[ED] = {
          edge.srcId = getId(JavaRuntime.getLong(oeBeginAddr + (curOffset << 4)))
          edge.attr = edatas.getData(curOffset)
          curOffset = activeEdgeSet.nextSetBit(curOffset + 1)
          edge
        }
      }
    }
  }

  override def tripletIterator[VD: ClassTag,ED : ClassTag](startLid : Long, endLid : Long, innerVertexDataStore: DataStore[VD], edatas : AbstractDataStore[ED], activeEdgeSet : BitSetWithOffset, edgeReversed : Boolean = false,
                                                           includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false, includeLid : Boolean = false)
  : Iterator[EdgeTriplet[VD, ED]] = {
    if (!edgeReversed){
      new Iterator[EdgeTriplet[VD, ED]] {
        var curOffset = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
        var curLid = startLid.toInt
        var srcId = innerVertexLid2Oid(curLid)
        var srcAttr :VD = innerVertexDataStore.getData(curLid)
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
            srcId = innerVertexLid2Oid(curLid)
            srcAttr = innerVertexDataStore.getData(curLid)
            true
          }
        }

        override def next(): EdgeTriplet[VD, ED] = {
          val edgeTriplet = new GSEdgeTripletImpl[VD, ED];
          val shift = (curOffset << 4)
          val dstLid = JavaRuntime.getLong(oeBeginAddr + shift).toInt
          edgeTriplet.eid = JavaRuntime.getLong(oeBeginAddr + shift + 8).toInt
          edgeTriplet.offset = curOffset
          edgeTriplet.dstId = getId(dstLid)
          edgeTriplet.dstAttr = innerVertexDataStore.getData(dstLid)
          edgeTriplet.srcId = srcId
          edgeTriplet.srcAttr = srcAttr
          edgeTriplet.attr = edatas.getData(curOffset)
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
        var dstId = innerVertexLid2Oid(curLid)
        var dstAttr :VD = innerVertexDataStore.getData(curLid)
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
            dstId = innerVertexLid2Oid(curLid)
            true
          }
        }

        override def next(): EdgeTriplet[VD, ED] = {
          val edgeTriplet = new GSEdgeTripletImpl[VD, ED];
          val shift = (curOffset << 4)
          val srcLid = JavaRuntime.getLong(oeBeginAddr + shift).toInt
          edgeTriplet.eid = JavaRuntime.getLong(oeBeginAddr + shift + 8).toInt
          edgeTriplet.offset = curOffset
          edgeTriplet.srcId = getId(srcLid)
          edgeTriplet.srcAttr = innerVertexDataStore.getData(srcLid)
          edgeTriplet.dstId = dstId
          edgeTriplet.dstAttr = dstAttr
          edgeTriplet.attr = edatas.getData(curOffset)
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
    require(vm.getInnerVertex(oid, vertex))
    require(vertex.GetValue() < ivnum)
    true
  }

  override def getOuterVertex(oid: Long, vertex: Vertex[Long]): Boolean = {
    require(vm.getOuterVertex(oid, vertex))
    require(vertex.GetValue() >= ivnum)
    true
  }

  override val structureType: GraphStructureType = GraphXFragmentStructure

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
      array(i) = getId(lid)
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
      array(i) = getId(beginNbr.vid().toInt)
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

  override def emptyIterateTriplets[VD: ClassTag, ED: ClassTag](startLid : Long, endLid : Long, vertexDataStore: DataStore[VD], edatas: AbstractDataStore[ED], activeSet: BitSetWithOffset, edgeReversed: Boolean, includeSrc: Boolean, includeDst: Boolean, newArray : AbstractDataStore[ED]): Unit = {
    val time0 = System.nanoTime()
    var curLid = startLid.toInt
    val edgeTriplet = new GSEdgeTripletImpl[VD, ED]
    var curOffset = activeSet.nextSetBit(activeSet.startBit)
    val vDataArray = vertexDataStore.asInstanceOf[AbstractInHeapDataStore[VD]].array
    if (curOffset < 0) return
    if (!edgeReversed){
      while (curLid < endLid && curOffset >= 0){
        val curEndOffset = getOEEndOffset(curLid)
        edgeTriplet.srcId = innerVertexLid2Oid(curLid)
        edgeTriplet.srcAttr = vDataArray(curLid)
        while (curOffset < curEndOffset && curOffset >= 0){
          val shift = curOffset << 4;
          val dstLid = JavaRuntime.getLong(oeBeginAddr + shift).toInt
          edgeTriplet.dstId = getId(dstLid)
          edgeTriplet.dstAttr = vDataArray(dstLid)
          edgeTriplet.attr = edatas.getData(curOffset)
          curOffset = activeSet.nextSetBit(curOffset + 1)
        }
        curLid += 1
      }
    }
    else {
      while (curLid < endLid){
        val curEndOffset = getOEEndOffset(curLid)
        edgeTriplet.dstId = innerVertexLid2Oid(curLid)
        edgeTriplet.dstAttr = vDataArray(curLid)
        while (curOffset < curEndOffset){
          val shift = curOffset << 4;
          val srcLid = JavaRuntime.getLong(oeBeginAddr + shift).toInt
          edgeTriplet.srcId = getId(srcLid)
          edgeTriplet.attr = edatas.getData(curOffset)
          edgeTriplet.srcAttr = vDataArray(srcLid)
          newArray.setData(curOffset,edgeTriplet.attr)
          curOffset = curOffset + 1
        }
        curLid += 1
      }
    }
    val time1 = System.nanoTime()
    log.info(s"Empty iterate triplets from ${startLid} to ${endLid} cost ${(time1 - time0) / 1000000} ms")
  }

  override def iterateTriplets[VD: ClassTag, ED: ClassTag,ED2 : ClassTag](startLid : Long, endLid : Long, f: EdgeTriplet[VD,ED] => ED2, activeVertices : BitSetWithOffset, vertexDataStore: DataStore[VD], edatas: AbstractDataStore[ED], activeSet: BitSetWithOffset, edgeReversed: Boolean, includeSrc: Boolean, includeDst: Boolean, resArray : AbstractDataStore[ED2]): Unit = {
    var curLid = activeVertices.nextSetBit(startLid.toInt)
    val edgeTriplet = new GSEdgeTripletImpl[VD, ED]
    var curOffset = activeSet.nextSetBit(activeSet.startBit)
//    val oldEDataArray = edatas.asInstanceOf[InHeapDataStore[ED]].array
//    val newEdataArray = resArray.asInstanceOf[InHeapDataStore[ED2]].array
    val vDataArray = vertexDataStore.asInstanceOf[AbstractInHeapDataStore[VD]].array
    if (!edgeReversed){
      while (curLid < endLid && curLid >= 0 && curOffset >= 0){
        val curEndOffset = getOEEndOffset(curLid)
        edgeTriplet.srcId = innerVertexLid2Oid(curLid)
        edgeTriplet.srcAttr = vDataArray(curLid)
        while (curOffset < curEndOffset && curOffset >= 0){
          val shift = curOffset << 4;
          val dstLid = JavaRuntime.getLong(oeBeginAddr + shift).toInt
          edgeTriplet.dstId = getId(dstLid)
          edgeTriplet.dstAttr = vDataArray(dstLid)
          edgeTriplet.attr = edatas.getData(curOffset)
          resArray.setData(curOffset, f(edgeTriplet))
          curOffset = activeSet.nextSetBit(curOffset + 1)
        }
        curLid = activeVertices.nextSetBit(curLid + 1)
      }
    }
    else {
      while (curLid < endLid && curLid >= 0 && curOffset >= 0){
        val curEndOffset = getOEEndOffset(curLid)
        edgeTriplet.dstId = innerVertexLid2Oid(curLid)
        edgeTriplet.dstAttr = vDataArray(curLid)
        while (curOffset < curEndOffset && curOffset >= 0){
          val shift = curOffset << 4;
          val srcLid = JavaRuntime.getLong(oeBeginAddr + shift).toInt
          edgeTriplet.srcId = getId(srcLid)
          edgeTriplet.srcAttr = vDataArray(srcLid)
          edgeTriplet.attr = edatas.getData(curOffset)
          resArray.setData(curOffset, f(edgeTriplet))
          curOffset = activeSet.nextSetBit(curOffset + 1)
        }
        curLid = activeVertices.nextSetBit(curLid + 1)
      }
    }
  }

  override def emptyIterateEdges[ED: ClassTag](startLid: VertexId, endLid: VertexId, edatas: AbstractDataStore[ED], activeSet: BitSetWithOffset, edgeReversed: Boolean, newArray : AbstractDataStore[ED]): Unit = {
    val time0 = System.nanoTime()
    var curLid = startLid.toInt
    val edge = new ReusableEdgeImpl[ED]
    var curOffset = activeSet.nextSetBit(activeSet.startBit)
//    val oldEdataArray = edatas.asInstanceOf[InHeapDataStore[ED]].array
//    val newEdataArray = newArray.asInstanceOf[InHeapDataStore[ED]].array
    if (curOffset < 0) return
    if (!edgeReversed){
      while (curLid < endLid && curOffset >= 0){
        val curEndOffset = getOEEndOffset(curLid)
        edge.srcId = innerVertexLid2Oid(curLid)
        while (curOffset < curEndOffset && curOffset >= 0){
          val shift = curOffset << 4;
          val dstLid = JavaRuntime.getLong(oeBeginAddr + shift).toInt
          edge.dstId = getId(dstLid)
          edge.attr = edatas.getData(curOffset)
//          newArray.setData(curOffset,f(edge))
          curOffset = activeSet.nextSetBit(curOffset + 1)
        }
        curLid += 1
      }
    }
    else {
      while (curLid < endLid){
        val curEndOffset = getOEEndOffset(curLid)
        edge.dstId = innerVertexLid2Oid(curLid)
        while (curOffset < curEndOffset){
          val shift = curOffset << 4;
          val srcLid = JavaRuntime.getLong(oeBeginAddr + shift).toInt
          edge.srcId = getId(srcLid)
          edge.attr = edatas.getData(curOffset)
          curOffset = curOffset + 1
        }
        curLid += 1
      }
    }
    val time1 = System.nanoTime()
    log.info(s"Empty iterate edges from ${startLid} to ${endLid} cost ${(time1 - time0)/1000000} ms")
  }


  override def iterateEdges[ED: ClassTag, ED2: ClassTag](startLid : Long, endLid : Long, f: Edge[ED] => ED2, edatas : AbstractDataStore[ED], activeEdgeSet: BitSetWithOffset, edgeReversed: Boolean, newArray: AbstractDataStore[ED2]): Unit = {
    val time0 = System.nanoTime()
    var curLid = startLid.toInt
    val edge = new ReusableEdgeImpl[ED]

    var curOffset = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
    log.info(s"start iterating edges, from ${startLid} to ${endLid}, ivnum ${vm.innerVertexSize()}, tvnum ${vm.getVertexSize}, oe offset len ${oeOffsetsArray.getLength}, oe offset end ${oeOffsetsArray.get(oeOffsetsLen-1)}")
    if (!edgeReversed){
      while (curLid < endLid){
        val curEndOffset = getOEEndOffset(curLid)
        val curBeginOffset = getOEBeginOffset(curLid)
        edge.srcId = innerVertexLid2Oid(curLid)
        var curOffset0 = curBeginOffset;
        while (curOffset0 < curEndOffset){
          val shift = curOffset0 * 16;
          val curAddr = oeBeginAddr + shift
          val dstLid = JavaRuntime.getLong(curAddr).toInt
          if (dstLid >= tvnum){
            throw new IllegalStateException("not possible lid" + dstLid + ",tvnum" + tvnum + ",curlid " + curLid + "oeBegin" + oeBeginAddr + ", cur address" + curAddr + "range, " + curBeginOffset + "," + curEndOffset)
          }
          curOffset0 += 1
        }
        curLid += 1
      }
      curLid = startLid.toInt
      while (curLid < endLid && curOffset >= 0){
        val curEndOffset = getOEEndOffset(curLid)
        edge.srcId = innerVertexLid2Oid(curLid)
        while (curOffset < curEndOffset && curOffset >= 0){
          val shift = curOffset * 16;
          val curAddr = oeBeginAddr + shift
          require(curAddr < oeEndAddr, s"cur addr ${curAddr}, oe end ${oeEndAddr}")
          val dstLid = JavaRuntime.getLong(curAddr).toInt
          if (dstLid >= tvnum){
            throw new IllegalStateException("not possible lid" + dstLid + ",tvnum" + tvnum)
          }
//          edge.dstId = getId(dstLid)
          if (dstLid < ivnum){
            require(dstLid < ivnum, s"index out of range ${dstLid}, ${ivnum}")
            edge.dstId = lid2Oid(myFid).get(dstLid)
          }
          else if (dstLid < tvnum) {
            require(dstLid >= ivnum && dstLid < tvnum, s"index out of range ${dstLid}, ${ivnum} ~ ${tvnum}")
            val gid = outerLid2Gid.get(dstLid - ivnum)
            val lid = idParser.getLocalId(gid)
            val fid = idParser.getFragId(gid)
            require(fid != myFid, s"outer fid can not equal to me ${fid}, ${myFid}, gid ${gid}")
            require(fid < fnum(), s"fid greater than fnum ${fid}, ${fnum()}")
            val len = lid2Oid(fid).getLength
            require(lid < len, s"lid should less than ivnum of frag ${fid}, lid ${lid}, len ${len}")
            edge.dstId = lid2Oid(fid).get(lid)
          }
          else {
            throw new IllegalStateException("not possible" + dstLid + ",tvnum" + tvnum)
          }
          edge.attr = edatas.getData(curOffset)
          newArray.setData(curOffset,f(edge))
          curOffset = activeEdgeSet.nextSetBit(curOffset + 1)
        }
        curLid += 1
      }
    }
    else {
      while (curLid < endLid && curOffset >= 0){
        val curEndOffset = getOEEndOffset(curLid)
        edge.dstId = innerVertexLid2Oid(curLid)
        while (curOffset < curEndOffset && curOffset >= 0){
          val shift = curOffset << 4;
          val srcLid = JavaRuntime.getLong(oeBeginAddr + shift).toInt
          edge.srcId = getId(srcLid)
          edge.attr = edatas.getData(curOffset)
          curOffset = activeEdgeSet.nextSetBit(curOffset + 1)
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

