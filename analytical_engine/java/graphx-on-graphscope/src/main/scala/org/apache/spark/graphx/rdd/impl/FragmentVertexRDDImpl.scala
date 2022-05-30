package org.apache.spark.graphx.rdd.impl

import org.apache.spark.OneToOneDependency
import org.apache.spark.graphx.{EdgeRDD, PartitionID, VertexId, VertexRDD}
import org.apache.spark.graphx.impl.ShippableVertexPartition
import org.apache.spark.graphx.rdd.FragmentVertexRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class FragmentVertexRDDImpl[VD] private[graphx](
                                                  @transient override val grapePartitionsRDD: RDD[FragmentVertexPartition[VD]],
                                                  val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                                  val outerVertexSynced : Boolean = false) // indicate whether we have done outer vertex shuffling.
                                                (implicit override protected val vdTag: ClassTag[VD])
  extends FragmentVertexRDD[VD](grapePartitionsRDD.context, List(new OneToOneDependency(grapePartitionsRDD)))  {
  override def count(): Long = {
    grapePartitionsRDD.map(_.fragment.getInnerVerticesNum).fold(0)(_ + _)
  }

  override private[graphx] def mapGrapeVertexPartitions[VD2: ClassTag](f: FragmentVertexPartition[VD] => FragmentVertexPartition[VD2]) = ???

  override private[graphx] def withGrapePartitionsRDD[VD2: ClassTag](partitionsRDD: RDD[FragmentVertexPartition[VD2]]) = ???

  override def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2): FragmentVertexRDD[VD2] = ???

  override def innerZipJoin[U: ClassTag, VD2: ClassTag](other: VertexRDD[U])(f: (VertexId, VD, U) => VD2): FragmentVertexRDD[VD2] = ???

  override def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])(f: (VertexId, VD, U) => VD2): FragmentVertexRDD[VD2] = ???

  override def leftJoin[VD2: ClassTag, VD3: ClassTag](other: RDD[(VertexId, VD2)])(f: (VertexId, VD, Option[VD2]) => VD3): FragmentVertexRDD[VD3] = ???

  override def leftZipJoin[VD2: ClassTag, VD3: ClassTag](other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): VertexRDD[VD3] = ???

  override def reindex(): VertexRDD[VD] = ???

  override private[graphx] def mapVertexPartitions[VD2](f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])(implicit evidence$1: ClassTag[VD2]) = ???

  override def mapValues[VD2](f: VD => VD2)(implicit evidence$2: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def mapValues[VD2](f: (VertexId, VD) => VD2)(implicit evidence$3: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def minus(other: RDD[(VertexId, VD)]): VertexRDD[VD] = ???

  override def minus(other: VertexRDD[VD]): VertexRDD[VD] = ???

  override def diff(other: RDD[(VertexId, VD)]): VertexRDD[VD] = ???

  override def diff(other: VertexRDD[VD]): VertexRDD[VD] = ???

  override def aggregateUsingIndex[VD2](messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2)(implicit evidence$12: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def reverseRoutingTables(): VertexRDD[VD] = ???

  override def withEdges(edges: EdgeRDD[_]): VertexRDD[VD] = ???

  override private[graphx] def withPartitionsRDD[VD2](partitionsRDD: RDD[ShippableVertexPartition[VD2]])(implicit evidence$13: ClassTag[VD2]) = ???

  override private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel) = ???

  override private[graphx] def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean) = ???

  override private[graphx] def shipVertexIds() = ???
}
