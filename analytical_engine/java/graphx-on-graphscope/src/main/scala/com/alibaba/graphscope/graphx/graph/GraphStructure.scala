package com.alibaba.graphscope.graphx.graph

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.GraphStructureType
import com.alibaba.graphscope.graphx.store.VertexDataStore
import com.alibaba.graphscope.graphx.utils.{ArrayWithOffset, BitSetWithOffset}
import org.apache.spark.graphx.{Edge, EdgeTriplet}
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

/**
 * Defines the interface of graph structure, include vm, csr. But doesn't contain vertex attribute
 * and edge attribute(or contained but we don't use)
 */
object GraphStructureTypes extends Enumeration{
  type GraphStructureType = Value
  val GraphXFragmentStructure,ArrowProjectedStructure = Value
}

trait GraphStructure extends Serializable {

  val structureType : GraphStructureType
  def inDegreeArray(startLid : Long, endLid : Long) :Array[Int]
  def outDegreeArray(startLid : Long, endLid : Long) : Array[Int]
  def inOutDegreeArray(startLid : Long, endLid : Long) : Array[Int]

  //of size (fnum, number of inner vertices which are outer vertices in frag i)
  val mirrorVertices : Array[BitSet]

  def iterator[ED : ClassTag](startLid : Long, endLid : Long, edatas : ArrayWithOffset[ED], activeSet: BitSetWithOffset, reversed : Boolean = false) : Iterator[Edge[ED]]

  def tripletIterator[VD: ClassTag,ED : ClassTag](startLid : Long, endLid : Long,vertexDataStore: VertexDataStore[VD],outerVertexDataStore: VertexDataStore[VD], edatas : ArrayWithOffset[ED],  activeSet: BitSetWithOffset,edgeReversed : Boolean = false, includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false, includeLid : Boolean = false): Iterator[EdgeTriplet[VD, ED]]

  def iterateEdges[ED : ClassTag,ED2 : ClassTag](startLid : Long, endLid : Long, f: Edge[ED] => ED2, edatas : ArrayWithOffset[ED], activeSet : BitSetWithOffset, edgeReversed : Boolean = false, newArray : ArrayWithOffset[ED2]) : Unit

  def iterateTriplets[VD : ClassTag, ED : ClassTag,ED2 : ClassTag](startLid : Long, endLid : Long,f : EdgeTriplet[VD,ED] => ED2,innerVertexDataStore: VertexDataStore[VD], outerVertexDataStore: VertexDataStore[VD], edatas : ArrayWithOffset[ED], activeSet : BitSetWithOffset, edgeReversed : Boolean = false, includeSrc : Boolean = true, includeDst : Boolean = true, newArray : ArrayWithOffset[ED2]) : Unit

   def getInDegree(vid: Long): Long

  /** get the oe begin offset */
  def getOEBeginOffset(vid: Long) : Long

  def getOEEndOffset(vid: Long) : Long

  /** get the oe begin offset */
  def getIEBeginOffset(vid: Long) : Long

  def getIEEndOffset(vid: Long) : Long

   def getOutDegree(vid: Long): Long

  def getOutNbrIds(vid : Long) : Array[Long]

  def getInNbrIds(vid : Long) : Array[Long]

  def getInOutNbrIds(vid : Long) : Array[Long]

   def isInEdgesEmpty(vid: Long): Boolean

   def isOutEdgesEmpty(vid: Long): Boolean

   def getInEdgesNum: Long

   def getOutEdgesNum: Long

   def fid(): Int

   def fnum(): Int

   def getId(vertex: Long): Long

   def getVertex(oid: Long, vertex: Vertex[Long]): Boolean

   def getTotalVertexSize: Long

   def getVertexSize: Long

   def getInnerVertexSize: Long

   def innerVertexLid2Oid(lid: Long): Long

   def outerVertexLid2Oid(lid: Long): Long

   def getOuterVertexSize: Long

   def innerOid2Gid(oid: Long): Long

   def getOuterVertexGid(lid: Long): Long

   def fid2GraphxPid(fid: Int): Int

   def outerVertexGid2Vertex(gid: Long, vertex: Vertex[Long]): Boolean

  def getInnerVertex(oid : Long, vertex: Vertex[Long]) : Boolean

  def getEids : Array[Long]

  def getOEOffsetRange(startLid : Long, endLid : Long) : (Long,Long)
}
