package org.apache.spark.graphx.impl.offheap

import com.alibaba.graphscope.graphx.JavaEdgePartition
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}
import org.apache.spark.graphx.{Edge, EdgeRDD, PartitionID, VertexId}
import org.apache.spark.graphx.impl.{EdgePartition, GrapeEdgePartition}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}

class OffHeapEdgeRDDImpl [VD: ClassTag, ED: ClassTag] private[graphx] (
                                                                        @transient val grapePartitionsRDD: RDD[(PartitionID, GrapeEdgePartition[VD, ED])],
                                                                        val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends EdgeRDD[ED](grapePartitionsRDD.context, List(new OneToOneDependency(grapePartitionsRDD))) {
  override private[graphx] def partitionsRDD = {
    throw new IllegalStateException("Not implemented")
  }
  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    val p = firstParent[(PartitionID, GrapeEdgePartition[VD, ED])].iterator(part, context)
    if (p.hasNext) {
//      p.next()._2.iterator.map(_.copy())
      p.next()._2.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }

  override def mapValues[ED2](f: Edge[ED] => ED2)(implicit evidence$1: ClassTag[ED2]): EdgeRDD[ED2] = ???

  override def reverse: EdgeRDD[ED] = ???

  override def innerJoin[ED2, ED3](other: EdgeRDD[ED2])(f: (VertexId, VertexId, ED, ED2) => ED3)(implicit evidence$2: ClassTag[ED2], evidence$3: ClassTag[ED3]): EdgeRDD[ED3] = ???

  override private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel) = ???
}
