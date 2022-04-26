package org.apache.spark.graphx.impl

import com.alibaba.graphscope.graphx.FragmentOps
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GrapeUtils.classToStr
import org.apache.spark.graphx.utils.SharedMemoryUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}

/**
 * Creating a graph abstraction by combining vertex RDD and edge RDD together.
 * Before doing this construction:
 *   - Both vertex RDD and edge RDD are available for map,fliter operators.
 *   - Both vertex RDD and edge RDD stores data in partitions
 *     When construct this graph, we will
 *   - copy data out to shared-memory
 *   - create mpi processes to load into fragment.
 *   - Wrap fragment as GrapeGraph when doing pregel computation.
 *     When changes made to graphx graph, it will not directly take effect on grape-graph. To apply these
 *     changes to grape-graph. Invoke to grape graph directly.
 *
 * The vertexRDD and EdgeRDD are designed in a way that, we can construct the whole graph without
 * any shuffle in MPI.
 *
 * @param vertices vertex rdd
 * @param edges    edge rdd
 * @tparam VD vd
 * @tparam ED ed
 */
class GrapeGraphImpl[VD: ClassTag, ED: ClassTag] protected(
                                                            @transient val vertices: GrapeVertexRDD[VD],
                                                            @transient val edges: GrapeEdgeRDD[ED]) extends Graph[VD, ED] with Serializable {

  protected def this() = this(null, null)

  val vdClass: Class[VD] = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
  val edClass: Class[ED] = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]

  def numVertices: Long = vertices.count()

  def numEdges: Long = edges.count()

  val sc = vertices.sparkContext



  //  def init = {
  //    //Write data to Memory mapped file.
  //    val vertexMappedSize = 32L * numVertices / numParitions + 128
  //    val edgeMappedSize = 32L * numEdges / numParitions + 128
  //    println("numPartitions: " + numParitions)
  //    println("reserve memory " + vertexMappedSize + " for per vertex file")
  //    println("reserve memory " + edgeMappedSize + " for per edge file")
  //    val vertexFileArray = vertices.mapToFile("graphx-vertex", vertexMappedSize)
  //    val edgeFileArray = edges.mapToFile("graphx-edge", edgeMappedSize) // actual 24
  //    println("map result for vertex: " + vertexFileArray.mkString("Array(", ", ", ")"))
  //    println("map result for edge : " + edgeFileArray.mkString("Array(", ", ", ")"))
  //    //Serialize the info to string, and pass it to mpi processes, which are launched to load the graph
  //    //to fragment
  //    this.fragIds = FragmentOps.graph2Fragment(vertexFileArray, edgeFileArray, vertexMappedSize, edgeMappedSize, !sc.isLocal, classToStr(vdClass), classToStr(edClass))
  //    println(s"Fragid: [${fragIds}]")
  //    //fragIds = 10001,111002,11003
  //    //Fetch back the fragment id, construct an object to hold fragment meta.
  //    //When pregel invoked, we will use this fragment for graph computing.
  //  }

  override val triplets: RDD[EdgeTriplet[VD, ED]] = null

  override def persist(newLevel: StorageLevel): Graph[VD, ED] = {
    vertices.persist(newLevel)
    edges.persist(newLevel)
    this
  }

  override def cache(): Graph[VD, ED] = {
    vertices.cache()
    edges.cache()
    this
  }

  override def checkpoint(): Unit = {
    vertices.checkpoint()
    edges.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    vertices.isCheckpointed && edges.isCheckpointed
  }

  override def getCheckpointFiles: Seq[String] = {
    Seq(vertices.getCheckpointFile, edges.getCheckpointFile).flatMap {
      case Some(path) => Seq(path)
      case None => Seq.empty
    }
  }

  override def unpersist(blocking: Boolean): Graph[VD, ED] = {
    vertices.unpersist(blocking)
    edges.unpersist(blocking)
    this
  }

  override def unpersistVertices(blocking: Boolean): Graph[VD, ED] = {
    vertices.unpersist(blocking)
    this
  }

  override def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED] = {
    throw new IllegalStateException("Unimplemented")
  }

  override def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: PartitionID): Graph[VD, ED] = {
    throw new IllegalStateException("Unimplemented")
  }

  override def mapVertices[VD2: ClassTag](f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    throw new IllegalStateException("Unimplemented")
  }

  override def mapEdges[ED2](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2])(implicit newEd: ClassTag[ED2]): Graph[VD, ED2] = {
    throw new IllegalStateException("Unimplemented")
  }

  override def mapTriplets[ED2](map: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2], tripletFields: TripletFields)(implicit newEd: ClassTag[ED2]): Graph[VD, ED2] = {
    throw new IllegalStateException("Unimplemented")
  }

  override def reverse: Graph[VD, ED] = {
    throw new IllegalStateException("Unimplemented")
  }

  override def subgraph(epred: EdgeTriplet[VD, ED] => Boolean, vpred: (VertexId, VD) => Boolean): Graph[VD, ED] = {
    throw new IllegalStateException("Unimplemented")
  }

  override def mask[VD2, ED2](other: Graph[VD2, ED2])(implicit evidence$9: ClassTag[VD2], evidence$10: ClassTag[ED2]): Graph[VD, ED] = {
    throw new IllegalStateException("Unimplemented")

  }

  override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = {
    throw new IllegalStateException("Unimplemented")

  }

  override private[graphx] def aggregateMessagesWithActiveSet[A](sendMsg: EdgeContext[VD, ED, A] => Unit, mergeMsg: (A, A) => A, tripletFields: TripletFields, activeSetOpt: Option[(VertexRDD[_], EdgeDirection)])(implicit evidence$12: ClassTag[A]) = {
    throw new IllegalStateException("Unimplemented")
  }

  override def outerJoinVertices[U, VD2](other: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit evidence$13: ClassTag[U], evidence$14: ClassTag[VD2], eq: VD =:= VD2): Graph[VD2, ED] = {
    throw new IllegalStateException("Unimplemented")
  }
}


object GrapeGraphImpl {
  def fromGraphXGraph[VD:ClassTag, ED: ClassTag](oldGraph: Graph[VD,ED]): GrapeGraphImpl[VD,ED] ={
    val sc = oldGraph.vertices.sparkContext
    val vdClass = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
    val edClass = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]
    require(oldGraph.isInstanceOf[GraphImpl[VD,ED]], "expect a graphImpl")
    val numVertices = oldGraph.numVertices
    val numEdges = oldGraph.numEdges //these are total edges
    val numParitions = oldGraph.edges.getNumPartitions

    val vertexMappedSize = 32L * numVertices  + 128
    val edgeMappedSize = 32L * numEdges  + 128

    println("numPartitions: v:" + oldGraph.vertices.getNumPartitions + ", e:" + oldGraph.edges.getNumPartitions)
    println("reserve memory " + vertexMappedSize + " for per vertex file, vertices: " + numVertices)
    println("reserve memory " + edgeMappedSize + " for per edge file, edges" + numEdges)
    val vertexFileArray = SharedMemoryUtils.mapVerticesToFile(oldGraph.vertices, "graphx-vertex", vertexMappedSize)
//    val vertexFileArray = vertices.mapToFile("graphx-vertex", vertexMappedSize)
//    val edgeFileArray = edges.mapToFile("graphx-edge", edgeMappedSize) // actual 24
    val edgeFileArray = SharedMemoryUtils.mapEdgesToFile(oldGraph.edges, "graphx-edge", edgeMappedSize)

    println("map result for vertex: " + vertexFileArray.mkString("Array(", ", ", ")"))
    println("map result for edge : " + edgeFileArray.mkString("Array(", ", ", ")"))
    //Serialize the info to string, and pass it to mpi processes, which are launched to load the graph
    //to fragment
    val fragIds = FragmentOps.graph2Fragment(vertexFileArray, edgeFileArray,
      vertexMappedSize, edgeMappedSize, !sc.isLocal, classToStr(vdClass), classToStr(edClass))
    println(s"Fragid: [${fragIds}]")
    //fragIds = 10001,111002,11003

    null
  }

  def toGraphXGraph[VD:ClassTag, ED : ClassTag](graph : Graph[VD,ED]) : Graph[VD,ED] = {
    null
  }
}
