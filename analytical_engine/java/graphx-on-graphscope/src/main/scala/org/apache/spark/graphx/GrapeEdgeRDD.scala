package org.apache.spark.graphx

import org.apache.spark.graphx.impl.{GrapeEdgePartition, GrapeEdgeRDDImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

abstract class GrapeEdgeRDD[ED](sc: SparkContext,
                                deps: Seq[Dependency[_]]) extends EdgeRDD[ED](sc, deps) {
  // scalastyle:off structural.type
  def grapePartitionsRDD: RDD[(PartitionID, GrapeEdgePartition[ED])]
  // scalastyle:on structural.type

  override protected def getPartitions: Array[Partition] = grapePartitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    val p = firstParent[(Int, GrapeEdgePartition[ED])].iterator(part, context)
    if (p.hasNext) {
      p.next()._2.iterator.map(_.copy())
    }
    else {
      Iterator.empty
    }
  }

  //  /**
  //   * Map the values in an edge partitioning preserving the structure but changing the values.
  //   *
  //   * @tparam ED2 the new edge value type
  //   * @param f the function from an edge to a new edge value
  //   * @return a new EdgeRDD containing the new edge values
  //   */
  //  def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgeRDD[ED2]

  //  /**
  //   * Reverse all the edges in this RDD.
  //   *
  //   * @return a new EdgeRDD containing all the edges reversed
  //   */
  //  def reverse: GrapeEdgeRDD[ED]

  //  /**
  //   * Inner joins this EdgeRDD with another EdgeRDD, assuming both are partitioned using the same
  //   * [[PartitionStrategy]].
  //   *
  //   * @param other the EdgeRDD to join with
  //   * @param f the join function applied to corresponding values of `this` and `other`
  //   * @return a new EdgeRDD containing only edges that appear in both `this` and `other`,
  //   *         with values supplied by `f`
  //   */
  //  def innerJoin[ED2: ClassTag, ED3: ClassTag]
  //  (other: EdgeRDD[ED2])
  //  (f: (VertexId, VertexId, ED, ED2) => ED3): EdgeRDD[ED3]

  //  /**
  //   * Changes the target storage level while preserving all other properties of the
  //   * EdgeRDD. Operations on the returned EdgeRDD will preserve this storage level.
  //   *
  //   * This does not actually trigger a cache; to do this, call
  //   * [[org.apache.spark.graphx.EdgeRDD#cache]] on the returned EdgeRDD.
  //   */
  //  private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel): EdgeRDD[ED]
}

object GrapeEdgeRDD {
  def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]): GrapeEdgeRDDImpl[ED] = {
    //Shuffle the edge rdd
    //then use build to build.
    null
  }

  def fromEdgePartitions[ED: ClassTag, VD: ClassTag](
        grapeEdgePartitions: RDD[(Int, GrapeEdgePartition[ED])]): GrapeEdgeRDDImpl[ED] = {
    new GrapeEdgeRDDImpl[ED](grapeEdgePartitions)
  }
}