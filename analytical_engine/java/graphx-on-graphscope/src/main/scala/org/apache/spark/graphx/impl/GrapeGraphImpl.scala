package org.apache.spark.graphx.impl

import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.impl.grape.{GrapeEdgeRDDImpl, GrapeVertexRDDImpl}
import org.apache.spark.graphx.impl.graph.{EdgeManagerImpl, VertexDataManagerImpl}
//import com.alibaba.graphscope.utils.FragmentRegistry
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GrapeUtils.{classToStr, generateForeignFragName, scalaClass2JavaClass}
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
                                                            @transient val edges: GrapeEdgeRDD[ED],
                                                            @transient val fragId : String) extends Graph[VD, ED] with Serializable {

  protected def this(vertices : GrapeVertexRDD[VD], edges : GrapeEdgeRDDImpl[VD,ED], fragId : String) = this(vertices,edges.asInstanceOf[GrapeEdgeRDD[ED]], fragId)

  val vdClass: Class[VD] = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
  val edClass: Class[ED] = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]
//  val grapeEdgeRDDImpl = edges.asInstanceOf[GrapeEdgeRDDImpl[VD,ED]]
  val grapeEdges: GrapeEdgeRDDImpl[VD, ED] = edges.asInstanceOf[GrapeEdgeRDDImpl[VD,ED]]
  val grapeVertices: GrapeVertexRDDImpl[VD] = vertices.asInstanceOf[GrapeVertexRDDImpl[VD]]

  def numVertices: Long = vertices.count()

  def numEdges: Long = edges.count()

  val sc = vertices.sparkContext

  /**
   * We need to combiner vertex attribute with edges to construct triplet, however, as vertex
   * attrs are split into different parttions, thus we need to gather them into one same holder,
   * vertex data manager.
   */
  @transient override lazy val triplets: RDD[EdgeTriplet[VD, ED]] = {
    val tmpEdgePartitionRDD = grapeEdges.grapePartitionsRDD.zipPartitions(grapeVertices.grapePartitionsRDD)(
      (edgeIter, vertexIter) => {
        val edgeTuple = edgeIter.next()
        val vertexTuple = vertexIter.next()
        /** update the vertex attr in [startLid, endLid), and return self */
        Iterator((edgeTuple._1, edgeTuple._2.aggregateVertexAttr(vertexTuple._2.startLid, vertexTuple._2.endLid, vertexTuple._2.values)))
      }
    )
    tmpEdgePartitionRDD.mapPartitions(
      edgeIter => {
        val edgeTuple = edgeIter.next()
        edgeTuple._2.tripletIterator()
      }
    )
  }

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
    throw new IllegalStateException("Currently grape graph doesn't support partition")
  }

  override def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: PartitionID): Graph[VD, ED] = {
    throw new IllegalStateException("Currently grape graph doesn't support partition")
  }

  override def mapVertices[VD2: ClassTag](f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    new GrapeGraphImpl[VD2,ED](vertices.mapGrapeVertexPartitions(_.map(f)), edges, fragId)
  }

  override def mapEdges[ED2](f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2])(implicit newEd: ClassTag[ED2]): Graph[VD, ED2] = {
    val newEdges = grapeEdges.mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    new GrapeGraphImpl[VD,ED2](vertices,newEdges, fragId)
  }

  override def mapTriplets[ED2: ClassTag](
       map: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2],
       tripletFields: TripletFields): Graph[VD, ED2] = {
    val grapeVertexRDDImpl = vertices.asInstanceOf[GrapeVertexRDDImpl[VD]]
    val tmpEdgePartitionRDD = grapeEdges.grapePartitionsRDD.zipPartitions(grapeVertices.grapePartitionsRDD)(
      (edgeIter, vertexIter) => {
        val edgeTuple = edgeIter.next()
        val vertexTuple = vertexIter.next()
        /** update the vertex attr in [startLid, endLid), and return self */
        Iterator((edgeTuple._1, edgeTuple._2.aggregateVertexAttr(vertexTuple._2.startLid, vertexTuple._2.endLid, vertexTuple._2.values)))
      }
    )
    val newPartitionRDD = tmpEdgePartitionRDD.mapPartitions({
      eIter => {
        if (eIter.hasNext){
          val (ePid, ePart) = eIter.next()
          val tripletIter = ePart.tripletIterator(tripletFields)
          Iterator((ePid,ePart.map(map(ePid, tripletIter))))
        }
        else {
          Iterator.empty
        }
      }
    })
    val newGrapeEdgeRDDImpl = grapeEdges.withPartitionsRDD(newPartitionRDD)
    new GrapeGraphImpl[VD,ED2](vertices, newGrapeEdgeRDDImpl, fragId)
  }

  override def reverse: Graph[VD, ED] = {
    new GrapeGraphImpl[VD,ED](vertices, edges.reverse.asInstanceOf[GrapeEdgeRDD[ED]], fragId)
  }

  override def subgraph(epred: EdgeTriplet[VD, ED] => Boolean, vpred: (VertexId, VD) => Boolean): Graph[VD, ED] = {
    val tmpEdgePartitionRDD = grapeEdges.grapePartitionsRDD.zipPartitions(grapeVertices.grapePartitionsRDD)(
      (edgeIter, vertexIter) => {
        val edgeTuple = edgeIter.next()
        val vertexTuple = vertexIter.next()
        /** update the vertex attr in [startLid, endLid), and return self */
        Iterator((edgeTuple._1, edgeTuple._2.aggregateVertexAttr(vertexTuple._2.startLid, vertexTuple._2.endLid, vertexTuple._2.values)))
      }
    )
    val newPartitionRDD = tmpEdgePartitionRDD.mapPartitions({
      eIter => {
        if (eIter.hasNext){
          val (ePid, ePart) = eIter.next()
          Iterator((ePid,ePart.filter(epred, vpred)))
        }
        else {
          Iterator.empty
        }
      }
    })
    val newGrapeEdgeRDDImpl = grapeEdges.withPartitionsRDD(newPartitionRDD)
    GrapeGraphImpl.fromRDDs(vertices, newGrapeEdgeRDDImpl, fragId)
  }

  override def mask[VD2, ED2](other: Graph[VD2, ED2])(implicit evidence$9: ClassTag[VD2], evidence$10: ClassTag[ED2]): Graph[VD, ED] = {
    val newVertices = vertices.innerJoin(other.vertices) {(vid, v, w) => v}
    val newEdges = edges.innerJoin(other.edges){ (src,dst,v,w) => v}
    GrapeGraphImpl.fromRDDs(newVertices, newEdges,fragId)
  }

  override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = {
    val newEdges = edges.asInstanceOf[GrapeEdgeRDDImpl[VD,ED]].mapEdgePartitions(
      (pid, part) => part.groupEdges(merge))
    new GrapeGraphImpl(vertices, newEdges, fragId)
  }

  override private[graphx] def aggregateMessagesWithActiveSet[A](sendMsg: EdgeContext[VD, ED, A] => Unit, mergeMsg: (A, A) => A, tripletFields: TripletFields, activeSetOpt: Option[(VertexRDD[_], EdgeDirection)])(implicit evidence$12: ClassTag[A]) = {
    throw new IllegalStateException("Unimplemented")
  }

  override def outerJoinVertices[U: ClassTag, VD2 : ClassTag](other: RDD[(VertexId, U)])
                                                             (mapFunc: (VertexId, VD, Option[U]) => VD2)
                                                             (implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    val newVertices = vertices.leftJoin(other)(mapFunc)
    /** if new vd2 differs from vertex data manager, create a vertex data manager */
    grapeEdges.grapePartitionsRDD.foreachPartition(iter => {
      val (pid, part) = iter.next()
      val vdCreator =  VertexDataManagerCreator.getOrCreate()
      vdCreator.create[VD,VD2,ED](pid, part.edgeManager.asInstanceOf[EdgeManagerImpl[VD,ED]].vertexDataManager)
    })
    val newEdges = grapeEdges.withPartitionsRDD(grapeEdges.grapePartitionsRDD.mapPartitions(iter => {
      val (pid, part) = iter.next()
      val vdCreator =  VertexDataManagerCreator.getOrCreate()
//      vdCreator.create[VD,VD2,ED](pid, .vertexDataManager)
      val oldEdgeManager = part.edgeManager.asInstanceOf[EdgeManagerImpl[VD,ED]]
      val newVdManager = vdCreator.get[VD2]
      Iterator((pid,part.withNewEdgeManager(oldEdgeManager.withNewVertexDataManager(newVdManager))))
    }))
    GrapeGraphImpl.fromRDDs[VD2, ED](newVertices, newEdges, fragId)
  }
}


object GrapeGraphImpl {
  def fromGraphXGraph[VD:ClassTag, ED: ClassTag](oldGraph: Graph[VD,ED], numCores: Int = 8): GrapeGraphImpl[VD,ED] ={
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
    val fragIds = MPIUtils.graph2Fragment(vertexFileArray, edgeFileArray,
      vertexMappedSize, edgeMappedSize, !sc.isLocal, classToStr(vdClass), classToStr(edClass))
    println(s"Fragid: [${fragIds}]")
    //fragIds = 10001,111002,11003
    val grapePartition = oldGraph.vertices.partitionsRDD.zipWithIndex().mapPartitions(iter => {
      if (iter.hasNext){
        val t= iter.next()
        val grapePid = FragmentRegistry.registFragment(fragIds, t._2.toInt);
        Iterator(grapePid)
      }
      else {
        Iterator.empty
      }
    })
    grapePartition.cache().count()
    grapePartition.foreachPartition(
      iter => {
        if (iter.hasNext){
          val pid = iter.next()
          FragmentRegistry.constructFragment[VD,ED](pid,generateForeignFragName(vdClass,edClass),
            scalaClass2JavaClass(vdClass).asInstanceOf[Class[VD]], scalaClass2JavaClass(edClass).asInstanceOf[Class[ED]], numCores)
        }
      })

    val grapeVertexPartitions = grapePartition.mapPartitions(
      iter => {
        if (iter.hasNext){
          val pid = iter.next();
          Iterator((pid,FragmentRegistry.getVertexPartition[VD](pid)))
        }
        else {
          Iterator.empty
        }
      }
    )

    val grapeEdgePartitions : RDD[(PartitionID, GrapeEdgePartition[VD,ED])] = grapePartition.mapPartitions(
      iter => {
        if (iter.hasNext){
          val pid = iter.next();
          Iterator((pid,FragmentRegistry.getEdgePartition[VD,ED](pid)))
        }
        else {
          Iterator.empty
        }
      }
    )
    println(s"total grape vertex partitions ${grapeVertexPartitions.count()}, edge partitions ${grapeEdgePartitions.count()}")
    //Construct grape vertex edge rdd from partitions.
    val grapeVertexRDD = GrapeVertexRDD.fromVertexPartitions(grapeVertexPartitions)
    val grapeEdgeRDD = GrapeEdgeRDD.fromEdgePartitions[VD,ED](grapeEdgePartitions)
    println(s"grape vertex rdd ${grapeVertexRDD.count()}, edge rdd ${grapeEdgeRDD.count()}")

    val resgraph = fromExistingRDDs(grapeVertexRDD, grapeEdgeRDD, getFragId(fragIds))
    println(s"after set ${resgraph.fragId}")
    resgraph
  }

  def fromExistingRDDs[VD: ClassTag,ED :ClassTag](vertices: GrapeVertexRDDImpl[VD], edges: GrapeEdgeRDDImpl[VD, ED], fragId : String): GrapeGraphImpl[VD,ED] ={
    new GrapeGraphImpl[VD,ED](vertices, edges, fragId)
  }

  def toGraphXGraph[VD:ClassTag, ED : ClassTag](graph : Graph[VD,ED]) : Graph[VD,ED] = {
    null
  }

  def fromRDDs[VD: ClassTag, ED : ClassTag](vertices : VertexRDD[VD], edges : EdgeRDD[ED], fragId :String) : GrapeGraphImpl[VD,ED] = {
    /** it is possible that the vd type bound with edges has changes in this operation, but edge rdd just casted */
    new GrapeGraphImpl[VD,ED](vertices.asInstanceOf[GrapeVertexRDDImpl[VD]], edges.asInstanceOf[GrapeEdgeRDDImpl[VD,ED]], fragId);
  }


  def getFragId(fragIds : String): String = {
    val fragsStrs = fragIds.split(",")
    println(s"frag str ${fragsStrs}")
    val hostName = GrapeUtils.getSelfHostName
    for (frag <- fragsStrs){
      println(s"for frag ${frag}")
      if (frag.startsWith(hostName)){
        val fragId = frag.substring(frag.indexOf(":") + 1, frag.size)
        println(s"matched ${fragId}")
        return fragId
      }
    }
    throw new IllegalStateException("Empty string found")
  }
}
