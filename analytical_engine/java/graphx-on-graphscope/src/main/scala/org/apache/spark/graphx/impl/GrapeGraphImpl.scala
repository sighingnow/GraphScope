package org.apache.spark.graphx.impl

import org.apache.spark.graphx.impl.grape.{GrapeEdgeRDDImpl, GrapeVertexRDDImpl}
import org.apache.spark.graphx.utils.ExecutorUtils
//import com.alibaba.graphscope.utils.FragmentRegistry
import org.apache.spark.graphx._
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

  protected def this(vertices : GrapeVertexRDDImpl[VD], edges : GrapeEdgeRDDImpl[VD,ED], fragId : String) =
    this(vertices.asInstanceOf[GrapeVertexRDD[VD]],edges.asInstanceOf[GrapeEdgeRDD[ED]])

  val vdClass: Class[VD] = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
  val edClass: Class[ED] = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]
  val grapeEdges: GrapeEdgeRDDImpl[VD, ED] = edges.asInstanceOf[GrapeEdgeRDDImpl[VD,ED]]
  val grapeVertices: GrapeVertexRDDImpl[VD] = vertices.asInstanceOf[GrapeVertexRDDImpl[VD]]

  def numVertices: Long = vertices.count()

  def numEdges: Long = edges.count()

  def generateGlobalVMIds() : Array[String] = {
    edges.grapePartitionsRDD.mapPartitions(iter => {
//      Iterator(iter.next()._2.vm.id())
      Iterator(ExecutorUtils.getHostName + ":" + iter.next()._2.vm.id())
    }).collect().distinct
  }

  def generateCSRIds() : Array[String] = {
    edges.grapePartitionsRDD.mapPartitions(iter => {
//      Iterator(ExecutorUtils.getHost2CSRID)
      Iterator(ExecutorUtils.getHostName + ":" + iter.next()._2.csr.id())
    }).collect().distinct
  }

  def generateVdataIds() : Array[String] = {
    vertices.grapePartitionsRDD.mapPartitions(iter => {
      Iterator(ExecutorUtils.getHostName + ":" + iter.next()._2.vertexData.id())
    }).collect().distinct
  }

  val sc = vertices.sparkContext

  /**
   * We need to combiner vertex attribute with edges to construct triplet, however, as vertex
   * attrs are split into different parttions, thus we need to gather them into one same holder,
   * vertex data manager.
   */
  @transient override lazy val triplets: RDD[EdgeTriplet[VD, ED]] = {
    null
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

  override def mapVertices[VD2 : ClassTag](map: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2): Graph[VD2, ED] = {
    new GrapeGraphImpl[VD2,ED](vertices.mapVertices(map), edges)
  }

  override def mapEdges[ED2](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2])(implicit evidence$5: ClassTag[ED2]): Graph[VD, ED2] = ???

  override def mapTriplets[ED2](map: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2], tripletFields: TripletFields)(implicit evidence$8: ClassTag[ED2]): Graph[VD, ED2] = ???

  override def reverse: Graph[VD, ED] = ???

  override def subgraph(epred: EdgeTriplet[VD, ED] => Boolean, vpred: (VertexId, VD) => Boolean): Graph[VD, ED] = ???

  override def mask[VD2, ED2](other: Graph[VD2, ED2])(implicit evidence$9: ClassTag[VD2], evidence$10: ClassTag[ED2]): Graph[VD, ED] = ???

  override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = ???

  override private[graphx] def aggregateMessagesWithActiveSet[A](sendMsg: EdgeContext[VD, ED, A] => Unit, mergeMsg: (A, A) => A, tripletFields: TripletFields, activeSetOpt: Option[(VertexRDD[_], EdgeDirection)])(implicit evidence$12: ClassTag[A]) = ???

  override def outerJoinVertices[U, VD2](other: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit evidence$13: ClassTag[U], evidence$14: ClassTag[VD2], eq: VD =:= VD2): Graph[VD2, ED] = ???
}


object GrapeGraphImpl {

  def fromExistingRDDs[VD: ClassTag,ED :ClassTag](vertices: GrapeVertexRDDImpl[VD], edges: GrapeEdgeRDDImpl[VD, ED]): GrapeGraphImpl[VD,ED] ={
    new GrapeGraphImpl[VD,ED](vertices, edges)
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
