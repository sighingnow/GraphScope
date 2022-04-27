package org.apache.spark.graphx.impl.offheap

import org.apache.spark.graphx.impl.ShippableVertexPartition
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * This class should be construct from off heap memories, which is provided by graphscope.
 * @param partitionsRDD
 * @param targetStorageLevel
 * @param vdTag
 * @tparam VD
 */
class OffHeapVertexRDDImpl[VD] private[graphx] (
                                                 @transient val grapePartitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD])],
                                                 val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
                                           (implicit override protected val vdTag: ClassTag[VD])
  extends GrapeVertexRDD[VD](grapePartitionsRDD.context, List(new OneToOneDependency(grapePartitionsRDD))) {

  override def reindex(): VertexRDD[VD] = {
    throw new IllegalStateException("Not implemented")
  }
  override protected def getPartitions: Array[Partition] = grapePartitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[(VertexId,VD)] = {
    val p = firstParent[(PartitionID, GrapeVertexPartition[VD])].iterator(part, context)
    if (p.hasNext) {
      //      p.next()._2.iterator.map(_.copy())
      p.next()._2.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }

  override private[graphx] def mapVertexPartitions[VD2](f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])(implicit evidence$1: ClassTag[VD2]) = {
    throw new IllegalStateException("Not implemented")
  }
  override private[graphx] def mapGrapeVertexPartitions[VD2: ClassTag](
                                                                   f: GrapeVertexPartition[VD] => GrapeVertexPartition[VD2])
  : GrapeVertexRDD[VD2] = {
    val newPartitionsRDD  = grapePartitionsRDD.mapPartitions(
      iter => {
        if (iter.hasNext){
          val tuple = iter.next();
          val pid = tuple._1
          val partition = tuple._2
          Iterator((pid, f.apply(partition)))
        }
        else {
          Iterator.empty
        }
      },preservesPartitioning = true
    )
    this.withGrapePartitionsRDD(newPartitionsRDD)
  }

  override def mapValues[VD2](f: VD => VD2)(implicit evidence$2: ClassTag[VD2]): VertexRDD[VD2] = {
    this.mapGrapeVertexPartitions(_.map((vid, attr) => f(attr)))
  }

  override def mapValues[VD2](f: (VertexId, VD) => VD2)(implicit evidence$3: ClassTag[VD2]): VertexRDD[VD2] = {
    this.mapGrapeVertexPartitions(_.map(f))
  }

  override def minus(other: RDD[(VertexId, VD)]): VertexRDD[VD] = {
    minus(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
  }

  override def minus(other: VertexRDD[VD]): VertexRDD[VD] = {
    throw new IllegalStateException("Not implemented")
  }

  override def diff(other: RDD[(VertexId, VD)]): VertexRDD[VD] = {
    diff(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
  }

  override def diff(other: VertexRDD[VD]): VertexRDD[VD] = {
    throw new IllegalStateException("Not implemented")
  }

  override def leftZipJoin[VD2, VD3](other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$4: ClassTag[VD2], evidence$5: ClassTag[VD3]): VertexRDD[VD3] = {
    throw new IllegalStateException("Not implemented")
  }

  override def leftJoin[VD2, VD3](other: RDD[(VertexId, VD2)])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$6: ClassTag[VD2], evidence$7: ClassTag[VD3]): VertexRDD[VD3] = {
    throw new IllegalStateException("Not implemented")
  }

  override def innerZipJoin[U, VD2](other: VertexRDD[U])(f: (VertexId, VD, U) => VD2)(implicit evidence$8: ClassTag[U], evidence$9: ClassTag[VD2]): VertexRDD[VD2] = {
    throw new IllegalStateException("Not implemented")
  }

  override def innerJoin[U, VD2](other: RDD[(VertexId, U)])(f: (VertexId, VD, U) => VD2)(implicit evidence$10: ClassTag[U], evidence$11: ClassTag[VD2]): VertexRDD[VD2] = {
    throw new IllegalStateException("Not implemented")
  }

  override def aggregateUsingIndex[VD2](messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2)(implicit evidence$12: ClassTag[VD2]): VertexRDD[VD2] = {
    throw new IllegalStateException("Not implemented")
  }

  override def reverseRoutingTables(): VertexRDD[VD] = {
    throw new IllegalStateException("Not implemented")
  }

  override def withEdges(edges: EdgeRDD[_]): VertexRDD[VD] = {
    throw new IllegalStateException("Not implemented")
  }

  override private[graphx] def withPartitionsRDD[VD2](partitionsRDD: RDD[ShippableVertexPartition[VD2]])(implicit evidence$13: ClassTag[VD2]) = {
    throw new IllegalStateException("not implemented")
  }

  override private[graphx] def withGrapePartitionsRDD[VD2 : ClassTag](partitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD2])]) : GrapeVertexRDD[VD2] = {
    new OffHeapVertexRDDImpl[VD2](partitionsRDD,this.targetStorageLevel)
  }

  override private[graphx] def withTargetStorageLevel(newTargetStorageLevel: StorageLevel) = {
    new OffHeapVertexRDDImpl[VD](grapePartitionsRDD, newTargetStorageLevel)
  }

  override private[graphx] def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean) = {
    throw new IllegalStateException("not implemented")  }

  override private[graphx] def shipVertexIds() = {
      throw new IllegalStateException("Not implemented")
    }

  override private[graphx] def partitionsRDD = {
    throw new IllegalStateException("Not implemented")
  }
}
