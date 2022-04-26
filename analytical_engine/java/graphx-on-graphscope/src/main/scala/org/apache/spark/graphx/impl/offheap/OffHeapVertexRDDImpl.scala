package org.apache.spark.graphx.impl.offheap

import org.apache.spark.OneToOneDependency
import org.apache.spark.graphx.impl.ShippableVertexPartition
import org.apache.spark.graphx.{EdgeRDD, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * This class should be construct from off heap memories, which is provided by graphscope.
 * @param partitionsRDD
 * @param targetStorageLevel
 * @param vdTag
 * @tparam VD
 */
class OffHeapVertexRDDImpl[VD] private[graphx] (
                                       @transient val partitionsRDD: RDD[ShippableVertexPartition[VD]],
                                             val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
                                           (implicit override protected val vdTag: ClassTag[VD])
  extends VertexRDD[VD](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {
  override def reindex(): VertexRDD[VD] = ???

  override private[graphx] def mapVertexPartitions[VD2](f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])(implicit evidence$1: ClassTag[VD2]) = ???

  override def mapValues[VD2](f: VD => VD2)(implicit evidence$2: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def mapValues[VD2](f: (VertexId, VD) => VD2)(implicit evidence$3: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def minus(other: RDD[(VertexId, VD)]): VertexRDD[VD] = ???

  override def minus(other: VertexRDD[VD]): VertexRDD[VD] = ???

  override def diff(other: RDD[(VertexId, VD)]): VertexRDD[VD] = ???

  override def diff(other: VertexRDD[VD]): VertexRDD[VD] = ???

  override def leftZipJoin[VD2, VD3](other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$4: ClassTag[VD2], evidence$5: ClassTag[VD3]): VertexRDD[VD3] = ???

  override def leftJoin[VD2, VD3](other: RDD[(VertexId, VD2)])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$6: ClassTag[VD2], evidence$7: ClassTag[VD3]): VertexRDD[VD3] = ???

  override def innerZipJoin[U, VD2](other: VertexRDD[U])(f: (VertexId, VD, U) => VD2)(implicit evidence$8: ClassTag[U], evidence$9: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def innerJoin[U, VD2](other: RDD[(VertexId, U)])(f: (VertexId, VD, U) => VD2)(implicit evidence$10: ClassTag[U], evidence$11: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def aggregateUsingIndex[VD2](messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2)(implicit evidence$12: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def reverseRoutingTables(): VertexRDD[VD] = ???

  override def withEdges(edges: EdgeRDD[_]): VertexRDD[VD] = ???

  override private[graphx] def withPartitionsRDD[VD2](partitionsRDD: RDD[ShippableVertexPartition[VD2]])(implicit evidence$13: ClassTag[VD2]) = ???

  override private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel) = ???

  override private[graphx] def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean) = ???

  override private[graphx] def shipVertexIds() = ???
}
