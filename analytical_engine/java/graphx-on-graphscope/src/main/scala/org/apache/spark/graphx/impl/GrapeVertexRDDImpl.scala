package org.apache.spark.graphx.impl

import org.apache.spark.OneToOneDependency
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class GrapeVertexRDDImpl[VD](
                              @transient val grapePartitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD])],
                              val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
                            (implicit override protected val vdTag: ClassTag[VD])
  extends GrapeVertexRDD[VD](grapePartitionsRDD.context, List(new OneToOneDependency(grapePartitionsRDD))) {

  /**
   * Just inherit but don't use.
   *
   * @return
   */
  override def partitionsRDD = null
  override def count(): Long = {
    grapePartitionsRDD.map(_._2.innerVertexNum.toLong).fold(0)(_ + _)
  }

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

  override  def withPartitionsRDD[VD2](partitionsRDD: RDD[ShippableVertexPartition[VD2]])(implicit evidence$13: ClassTag[VD2]): Null = {
    null
  }

  override def withTargetStorageLevel(targetStorageLevel: StorageLevel): GrapeVertexRDDImpl[VD] = {
    new GrapeVertexRDDImpl[VD](grapePartitionsRDD, targetStorageLevel)
  }

  override def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, VertexAttributeBlock[VD])] = ???

  override private[graphx] def shipVertexIds() = {
    null
  }
}
