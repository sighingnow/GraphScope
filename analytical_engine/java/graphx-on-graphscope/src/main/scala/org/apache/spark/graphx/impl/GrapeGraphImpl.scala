package org.apache.spark.graphx.impl

import org.apache.spark.graphx.{Edge, EdgeContext, EdgeDirection, EdgeTriplet, GrapeEdgeRDD, GrapeVertexRDD, Graph, PartitionID, PartitionStrategy, TripletFields, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class GrapeGraphImpl[VD :ClassTag, ED: ClassTag] protected (
         @transient  val vertices: GrapeVertexRDD[VD],
         @transient val edges: GrapeEdgeRDD[ED]) extends Graph[VD, ED] with Serializable{

  protected def this() = this(null, null)
  def numVertices : Long = vertices.count()
  def numEdges : Long = edges.count()

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

  override def mapVertices[VD2](map: (VertexId, VD) => VD2)(implicit evidence$3: ClassTag[VD2], eq: VD =:= VD2): Graph[VD2, ED] = {
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


object GrapeGraphImpl{

  def fromExistingRDDs[VD: ClassTag, ED: ClassTag](
                              vertices: GrapeVertexRDD[VD],
                              edges: GrapeEdgeRDD[ED]): GrapeGraphImpl[VD, ED] = {
    new GrapeGraphImpl[VD,ED](vertices, edges)
  }
  def fromEdgeRDD[VD: ClassTag, ED: ClassTag](
      edgeRdd: GrapeEdgeRDDImpl[ED],
      defaultVertexAttr: VD,
      edgeStorageLevel : StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel= StorageLevel.MEMORY_ONLY): GrapeGraphImpl[VD, ED] = {
    val edgesCached = edgeRdd.withTargetStorageLevel(edgeStorageLevel).cache()
    val vertices =
      GrapeVertexRDD.fromEdges(edgesCached, edgesCached.partitions.length, defaultVertexAttr)
        .withTargetStorageLevel(vertexStorageLevel)
    fromExistingRDDs(vertices, edgesCached)
  }
}