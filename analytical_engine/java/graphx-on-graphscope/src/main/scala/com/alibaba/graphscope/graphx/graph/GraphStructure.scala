package com.alibaba.graphscope.graphx.graph

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.GraphStructureType
import com.alibaba.graphscope.graphx.store.VertexDataStore
import com.alibaba.graphscope.utils.array.PrimitiveArray
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
  val inDegreeArray :PrimitiveArray[Int]
  val outDegreeArray : PrimitiveArray[Int]
  val inOutDegreeArray : PrimitiveArray[Int]

  //of size (fnum, number of inner vertices which are outer vertices in frag i)
  val mirrorVertices : Array[Array[Long]]

  def iterator[ED : ClassTag](edatas : PrimitiveArray[ED], activeSet: BitSet, reversed : Boolean = false) : Iterator[Edge[ED]]

  def tripletIterator[VD: ClassTag,ED : ClassTag](vertexDataStore: VertexDataStore[VD], edatas : PrimitiveArray[ED],  activeSet: BitSet,edgeReversed : Boolean = false, includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false): Iterator[EdgeTriplet[VD, ED]]

   def getInDegree(vid: Long): Long

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

  def getEids : PrimitiveArray[Long]
}
