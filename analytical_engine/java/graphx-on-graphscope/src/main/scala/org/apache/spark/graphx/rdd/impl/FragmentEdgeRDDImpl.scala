package org.apache.spark.graphx.rdd.impl

import org.apache.spark.OneToOneDependency
import org.apache.spark.graphx.rdd.{FragmentEdgeRDD, FragmentVertexRDD}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class FragmentEdgeRDDImpl [VD: ClassTag, ED: ClassTag] private[graphx](@transient override val grapePartitionsRDD: RDD[(FragmentEdgePartition[VD, ED])],
                                                                       val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends FragmentEdgeRDD[ED](grapePartitionsRDD.context, List(new OneToOneDependency(grapePartitionsRDD))) {

  override def count(): Long = {
    grapePartitionsRDD.map(_.fragment.getEdgeNum).fold(0)(_ + _)
  }

  override def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): FragmentEdgeRDD[ED2] = ???

  override def innerJoin[ED2: ClassTag, ED3: ClassTag](other: EdgeRDD[ED2])(f: (VertexId, VertexId, ED, ED2) => ED3): FragmentEdgeRDD[ED3] = ???

  override def reverse: FragmentEdgeRDD[ED] = ???

  override def generateDegreeRDD(originalVertexRDD: FragmentVertexRDD[_], edgeDirection: EdgeDirection): FragmentVertexRDD[PartitionID] = ???

  override private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel) = ???
}
