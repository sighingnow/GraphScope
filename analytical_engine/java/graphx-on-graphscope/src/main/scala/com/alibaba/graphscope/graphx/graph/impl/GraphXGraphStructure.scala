package com.alibaba.graphscope.graphx.graph.impl

import com.alibaba.graphscope.ds.{ImmutableTypedArray, Vertex}
import com.alibaba.graphscope.graphx.graph.{GSEdgeTriplet, GSEdgeTripletImpl, GraphStructure, ReusableEdge, ReusableEdgeImpl, ReverseGSEdgeTripletImpl, ReversedReusableEdge}
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.{GraphStructureType, GraphXFragmentStructure}
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.impl.partition.data.VertexDataStore
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

//import scala.collection.BitSet
import scala.reflect.ClassTag

class GraphXGraphStructure(val vm : GraphXVertexMap[Long,Long], val csr : GraphXCSR[Long],  var srcLids : PrimitiveArray[Long],
                           val dstLids : PrimitiveArray[Long], val srcOids : PrimitiveArray[Long],
                           val dstOids : PrimitiveArray[Long],
                           val eids : PrimitiveArray[Long]) extends GraphStructure with Logging{
  val startLid = 0
  val endLid: Long = vm.innerVertexSize()

  lazy val inDegreeArray: PrimitiveArray[Int] = getInDegreeArray
  lazy val outDegreeArray: PrimitiveArray[Int] = getOutDegreeArray
  lazy val inOutDegreeArray: PrimitiveArray[Int] = getInOutDegreeArray

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

  private def getOutDegreeArray : PrimitiveArray[Int] = {
    val time0 = System.nanoTime()
    val len = vm.getVertexSize.toInt
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
    val len = vm.getVertexSize.toInt
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
    val len = vm.getVertexSize.toInt
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

//  override def getInDegree(vid: Long): Long = csr.getInDegree(vid)
//
//  override def getOutDegree(vid: Long): Long = csr.getOutDegree(vid)

  override def isInEdgesEmpty(vid: Long): Boolean = csr.isInEdgesEmpty(vid)

  override def isOutEdgesEmpty(vid: Long): Boolean = csr.isOutEdgesEmpty(vid)

  override def getInEdgesNum: Long = csr.getInEdgesNum

  override def getOutEdgesNum: Long = csr.getOutEdgesNum

  override def fid(): Int = vm.fid()

  override def fnum(): Int = vm.fnum()

  override def getId(vertex: Long): Long = vm.getId(vertex)

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

  override def iterator[ED: ClassTag](edatas: PrimitiveArray[ED], activeEdgeSet : BitSet, edgeReversed : Boolean = false): Iterator[Edge[ED]] = {
      new Iterator[Edge[ED]]{
        var offset : Long = activeEdgeSet.nextSetBit(0)
        var edge: ReusableEdge[ED] = null.asInstanceOf[ReusableEdge[ED]]

        if (edgeReversed){
          edge = new ReversedReusableEdge[ED];
        }
        else {
          edge = new ReusableEdgeImpl[ED];
        }
        override def hasNext: Boolean = {
          offset >= 0
        }

        override def next() : Edge[ED] = {
          edge.srcId = srcOids.get(offset)
          edge.dstId = dstOids.get(offset)
          edge.attr = edatas.get(eids.get(offset))
          edge.index = offset
          offset = activeEdgeSet.nextSetBit(offset.toInt + 1)
          edge
        }
    }
  }

  override def tripletIterator[VD: ClassTag,ED : ClassTag](vertexDataStore: VertexDataStore[VD],edatas : PrimitiveArray[ED], activeEdgeSet : BitSet, edgeReversed : Boolean = false,
                      includeSrc: Boolean = true, includeDst: Boolean = true)
  : Iterator[EdgeTriplet[VD, ED]] = new Iterator[EdgeTriplet[VD, ED]] {

    var offset : Long = activeEdgeSet.nextSetBit(0)

    def createTriplet : GSEdgeTriplet[VD,ED] = {
      if (!edgeReversed){
        new GSEdgeTripletImpl[VD,ED];
      }
      else {
        new ReverseGSEdgeTripletImpl[VD,ED]
      }
    }

    override def hasNext: Boolean = {
      offset >= 0
    }

    override def next() : EdgeTriplet[VD,ED] = {
      //Find srcLid of curNbr
      val edgeTriplet = createTriplet
      //      log.info(s"curLid ${curLid},offset ${offset}, offset limit${offsetLimit}")
      edgeTriplet.index = offset
      edgeTriplet.srcId = srcOids.get(offset)
      edgeTriplet.dstId = dstOids.get(offset)
      edgeTriplet.attr = edatas.get(eids.get(offset))
      edgeTriplet.srcAttr = vertexDataStore.getData(srcLids.get(offset))
      edgeTriplet.dstAttr = vertexDataStore.getData(dstLids.get(offset))
      offset = activeEdgeSet.nextSetBit(offset.toInt + 1)
      edgeTriplet
    }
  }

  override def getInnerVertex(oid: Long, vertex: Vertex[Long]): Boolean = {
    require(vm.getVertex(oid, vertex))
    require(vertex.GetValue() < vm.innerVertexSize())
    true
  }

  override val structureType: GraphStructureType = GraphXFragmentStructure
}
