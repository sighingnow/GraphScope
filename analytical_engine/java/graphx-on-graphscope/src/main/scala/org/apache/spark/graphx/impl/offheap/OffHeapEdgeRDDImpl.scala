package org.apache.spark.graphx.impl.offheap

import com.alibaba.graphscope.graphx.JavaEdgePartition
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}
import org.apache.spark.graphx.{Edge, EdgeRDD, GrapeEdgeRDD, PartitionID, VertexId}
import org.apache.spark.graphx.impl.{EdgePartition, EdgeRDDImpl, GrapeEdgePartition}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}

class OffHeapEdgeRDDImpl [VD: ClassTag, ED: ClassTag] private[graphx] (
                                                                        @transient val grapePartitionsRDD: RDD[(PartitionID, GrapeEdgePartition[VD, ED])],
                                                                        val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends EdgeRDD[ED](grapePartitionsRDD.context, List(new OneToOneDependency(grapePartitionsRDD))) {

  override protected def getPartitions: Array[Partition] = grapePartitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    val p = firstParent[(PartitionID, GrapeEdgePartition[VD, ED])].iterator(part, context)
    if (p.hasNext) {
      p.next()._2.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }

  override def setName(_name: String): this.type = {
    if (grapePartitionsRDD.name != null) {
      grapePartitionsRDD.setName(grapePartitionsRDD.name + ", " + _name)
    } else {
      grapePartitionsRDD.setName(_name)
    }
    this
  }
  setName("OffHeapGrapeEdgeRDD")

  /**
   * The data in offheap edge rdd is not split by partitioner
   */
  override val partitioner = null

  override def collect(): Array[Edge[ED]] = this.map(_.copy()).collect()

  override def persist(newLevel: StorageLevel): this.type = {
    grapePartitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = false): this.type = {
    grapePartitionsRDD.unpersist(blocking)
    this
  }

  override def cache(): this.type = {
    grapePartitionsRDD.persist(targetStorageLevel)
    this
  }

  override def getStorageLevel: StorageLevel = grapePartitionsRDD.getStorageLevel

  override def checkpoint(): Unit = {
    grapePartitionsRDD.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    firstParent[(PartitionID, GrapeEdgePartition[VD, ED])].isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    grapePartitionsRDD.getCheckpointFile
  }

  override def count(): Long = {
    grapePartitionsRDD.map(_._2.numEdges.toLong).fold(0)(_ + _)
  }
//  override def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): EdgeRDDImpl[ED2, VD] = {
//    throw new NotImplementedError("this inherited method is not implemented")
//  }

  override def mapValues[ED2 :ClassTag](f: Edge[ED] => ED2): OffHeapEdgeRDDImpl[VD,ED2] = {
    mapEdgePartitions((pid, part) => part.map(f))
  }

  override def reverse: EdgeRDD[ED] = ???

  override def innerJoin[ED2, ED3](other: EdgeRDD[ED2])(f: (VertexId, VertexId, ED, ED2) => ED3)(implicit evidence$2: ClassTag[ED2], evidence$3: ClassTag[ED3]): EdgeRDD[ED3] = ???

  override private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel) = ???

  override private[graphx] def partitionsRDD = {
    throw new IllegalStateException("Not implemented")
  }

  def mapEdgePartitions[VD2: ClassTag, ED2: ClassTag](
           f: (PartitionID, GrapeEdgePartition[VD, ED]) => GrapeEdgePartition[VD2, ED2]): OffHeapEdgeRDDImpl[VD2, ED2] = {
    this.withPartitionsRDD[VD2, ED2](grapePartitionsRDD.mapPartitions({ iter =>
      if (iter.hasNext) {
        val (pid, ep) = iter.next()
        Iterator((pid, f(pid, ep)))
      } else {
        Iterator.empty
      }
    }, preservesPartitioning = true))
  }

  private[graphx] def withPartitionsRDD[VD2: ClassTag, ED2: ClassTag](
          partitionsRDD: RDD[(PartitionID, GrapeEdgePartition[VD2, ED2])]): OffHeapEdgeRDDImpl[VD2, ED2] = {
    new OffHeapEdgeRDDImpl[VD2,ED2](partitionsRDD, this.targetStorageLevel)
  }
}
