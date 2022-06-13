package com.alibaba.graphscope.graphx.graph.impl

import com.alibaba.graphscope.ds.{PropertyNbrUnit, Vertex}
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor
import com.alibaba.graphscope.fragment.{ArrowFragment, ArrowProjectedFragment, IFragment}
import com.alibaba.graphscope.graphx.graph.GraphStructure
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.{ArrowProjectedStructure, GraphStructureType}
import com.alibaba.graphscope.graphx.{GSEdgeTriplet, GSEdgeTripletImpl, ReverseGSEdgeTripletImpl}
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.impl.partition.data.VertexDataStore
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

class FragmentStructure(val fragment : IFragment[Long,Long,_,_]) extends GraphStructure with Logging{
  val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
  val fid2Pid = new Array[Int](fragment.fnum())

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
//    log.info(s"${res.mkString("Array(", ", ", ")")}")
    res
  }

  private def getOutDegreeArray : PrimitiveArray[Int] = {
    val arrowFragment : ArrowFragment[Long] = null
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


  override def vertexNum(): Long = fragment.getVerticesNum

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
    if (fragment.fragmentType().equals(ArrowProjectedAdaptor.fragmentType)){
      val projectedFragment = fragment.asInstanceOf[ArrowProjectedAdaptor[Long,Long,_,ED]]
      if (edatas == null){
        newProjectedIterator(projectedFragment.getArrowProjectedFragment.asInstanceOf[ArrowProjectedFragment[Long,Long,_,ED]],activeSet,edgeReversed)
      }
      else {
        newProjectedIteratorV2(projectedFragment.getArrowProjectedFragment.asInstanceOf[ArrowProjectedFragment[Long,Long,_,ED]],edatas,activeSet,edgeReversed)
      }
    }
    else {
      throw new IllegalStateException("Not implemented")
    }
  }

  override def tripletIterator[VD: ClassTag, ED: ClassTag](vertexDataStore: VertexDataStore[VD], edatas: PrimitiveArray[ED], activeSet: BitSet, includeSrc: Boolean, includeDst: Boolean, edgeReversed : Boolean): Iterator[EdgeTriplet[VD, ED]] = {
    if (fragment.fragmentType().equals(ArrowProjectedAdaptor.fragmentType)){
      val projectedFragment = fragment.asInstanceOf[ArrowProjectedAdaptor[Long,Long,VD,ED]].getArrowProjectedFragment.asInstanceOf[ArrowProjectedFragment[Long,Long,VD,ED]]
      if (edatas == null){
        newProjectedTripletIterator(projectedFragment, vertexDataStore,activeSet,edgeReversed,includeSrc,includeDst)
      }
      else {
        newProjectedTripletIteratorV2(projectedFragment, vertexDataStore,edatas,activeSet,edgeReversed,includeSrc,includeDst)
      }
    }
    else {
      throw new IllegalStateException("Not implemented")
    }
  }

  private def newProjectedTripletIterator[VD: ClassTag,ED : ClassTag](frag: ArrowProjectedFragment[Long, Long, VD, ED],vertexDataStore: VertexDataStore[VD], bitSet: BitSet, edgeReversed: Boolean, includeSrc : Boolean, includeDst : Boolean) : Iterator[EdgeTriplet[VD,ED]] = {
    new Iterator[EdgeTriplet[VD,ED]]{
      def createTriplet : GSEdgeTriplet[VD,ED] = {
        if (!edgeReversed){
          new GSEdgeTripletImpl[VD,ED];
        }
        else {
          new ReverseGSEdgeTripletImpl[VD,ED]
        }
      }
      var curLid = 0
      val endLid : Long = frag.getInnerVerticesNum
      var offset : Long = bitSet.nextSetBit(0)
      val NBR_SIZE = 16
      val edataArrayAccessor = frag.getEdataArrayAccessor
      vertex.SetValue(curLid)
      val nbr : PropertyNbrUnit[Long]= frag.getOutEdgesPtr
      val initAddress : Long = nbr.getAddress

      override def hasNext: Boolean = {
        if (offset < 0) return false

        while (lid2Offset(curLid + 1) <= offset) {
          curLid += 1
        }
        true
      }

      override def next(): EdgeTriplet[VD,ED] = {
        val edge = createTriplet
        nbr.setAddress(initAddress + NBR_SIZE * offset)
        val dstLid = nbr.vid()
        val attr = edataArrayAccessor.get(nbr.eid())
        vertex.SetValue(curLid)
        edge.srcId = frag.getInnerVertexId(vertex)
        if (includeSrc){
//          edge.srcAttr = frag.getData(vertex)
          edge.srcAttr = vertexDataStore.getData(curLid)
        }
        vertex.SetValue(dstLid)
        edge.dstId = frag.getId(vertex)
        if (includeDst){
//          edge.dstAttr = frag.getData(vertex)
          edge.dstAttr = vertexDataStore.getData(dstLid)
        }
        edge.attr = attr
	      edge.index = offset
        offset = bitSet.nextSetBit((offset + 1).toInt)
        edge
      }
    }
  }

  private def newProjectedIterator[ED : ClassTag](frag: ArrowProjectedFragment[Long, Long, _, ED], bitSet: BitSet, edgeReversed : Boolean) : Iterator[Edge[ED]] = {
    new Iterator[Edge[ED]]{
      var curLid = 0
      val endLid : Long = frag.getInnerVerticesNum
      var edge: ReusableEdge[ED] = null.asInstanceOf[ReusableEdge[ED]]
      if (edgeReversed){
        edge = new ReversedReusableEdge[ED];
      }
      else {
        edge = new ReusableEdgeImpl[ED];
      }
      var offset : Long = bitSet.nextSetBit(0)
      val NBR_SIZE = 16
      val edataArrayAccessor = frag.getEdataArrayAccessor
      vertex.SetValue(curLid)
      val nbr : PropertyNbrUnit[Long]= frag.getOutEdgesPtr
      val initAddress : Long = nbr.getAddress

      override def hasNext: Boolean = {
        if (offset < 0) return false

        while (lid2Offset(curLid + 1) <= offset) {
//          log.info(s"inc curLid since ${lid2Offset(curLid + 1)} leq ${offset}")
          curLid += 1
        }
        vertex.SetValue(curLid)
        edge.srcId = frag.getInnerVertexId(vertex)
        true
      }

      override def next(): Edge[ED] = {
        nbr.setAddress(initAddress + NBR_SIZE * offset)
        val dstLid = nbr.vid()
        val attr = edataArrayAccessor.get(nbr.eid())
//        log.info(s"src lid ${curLid}, src oid ${edge.srcId}  eid ${nbr.eid()}, attr ${attr}")
        vertex.SetValue(dstLid)
        edge.dstId = frag.getId(vertex)
        edge.attr = attr
	      edge.index = offset
        offset = bitSet.nextSetBit((offset + 1).toInt)
        edge
      }
    }
  }

  private def newProjectedIteratorV2[ED : ClassTag](frag: ArrowProjectedFragment[Long, Long, _, ED], edatas : PrimitiveArray[ED], bitSet: BitSet, edgeReverse : Boolean) : Iterator[Edge[ED]] = {
    new Iterator[Edge[ED]]{
      var curLid = 0
      val endLid : Long = frag.getInnerVerticesNum
      var edge: ReusableEdge[ED] = null.asInstanceOf[ReusableEdge[ED]]
      if (edgeReverse){
        edge = new ReversedReusableEdge[ED];
      }
      else {
        edge = new ReusableEdgeImpl[ED];
      }
      var offset : Long = bitSet.nextSetBit(0)
      val NBR_SIZE = 16
      vertex.SetValue(curLid)
      val nbr : PropertyNbrUnit[Long]= frag.getOutEdgesPtr
      val initAddress : Long = nbr.getAddress

      override def hasNext: Boolean = {
        if (offset < 0) return false

        while (lid2Offset(curLid + 1) <= offset) {
          curLid += 1
        }
        vertex.SetValue(curLid)
        edge.srcId = frag.getInnerVertexId(vertex)
        true
      }

      override def next(): Edge[ED] = {
        nbr.setAddress(initAddress + NBR_SIZE * offset)
        val dstLid = nbr.vid()
        val attr = edatas.get(offset)
        vertex.SetValue(dstLid)
        edge.dstId = frag.getId(vertex)
        edge.attr = attr
        edge.index = offset
        offset = bitSet.nextSetBit((offset + 1).toInt)
        edge
      }
    }
  }
  private def newProjectedTripletIteratorV2[VD: ClassTag,ED : ClassTag](frag: ArrowProjectedFragment[Long, Long, VD, ED],vertexDataStore: VertexDataStore[VD], edatas : PrimitiveArray[ED],bitSet: BitSet, edgeReversed: Boolean, includeSrc : Boolean, includeDst : Boolean) : Iterator[EdgeTriplet[VD,ED]] = {
    new Iterator[EdgeTriplet[VD,ED]]{
      def createTriplet : GSEdgeTriplet[VD,ED] = {
        if (!edgeReversed){
          new GSEdgeTripletImpl[VD,ED];
        }
        else {
          new ReverseGSEdgeTripletImpl[VD,ED]
        }
      }
      var curLid = 0
      val endLid : Long = frag.getInnerVerticesNum
      var offset : Long = bitSet.nextSetBit(0)
      val NBR_SIZE = 16
      vertex.SetValue(curLid)
      val nbr : PropertyNbrUnit[Long]= frag.getOutEdgesPtr
      val initAddress : Long = nbr.getAddress

      override def hasNext: Boolean = {
        if (offset < 0) return false

        while (lid2Offset(curLid + 1) <= offset) {
          curLid += 1
        }
        true
      }

      override def next(): EdgeTriplet[VD,ED] = {
        val edge = createTriplet
        nbr.setAddress(initAddress + NBR_SIZE * offset)
        val dstLid = nbr.vid()
        val attr = edatas.get(offset)
        vertex.SetValue(curLid)
        edge.srcId = frag.getInnerVertexId(vertex)
        if (includeSrc){
//          edge.srcAttr = frag.getData(vertex)
          edge.srcAttr = vertexDataStore.getData(curLid)
        }
        vertex.SetValue(dstLid)
        edge.dstId = frag.getId(vertex)
        if (includeDst){
//          edge.dstAttr = frag.getData(vertex)
          edge.dstAttr = vertexDataStore.getData(dstLid)
        }
        edge.attr = attr
	      edge.index = offset
        offset = bitSet.nextSetBit((offset + 1).toInt)
        edge
      }
    }
  }

  override def getInnerVertex(oid: Long, vertex: Vertex[Long]): Boolean = {
    require(fragment.getInnerVertex(oid,vertex))
    true
  }

  override val structureType: GraphStructureType = ArrowProjectedStructure
}
