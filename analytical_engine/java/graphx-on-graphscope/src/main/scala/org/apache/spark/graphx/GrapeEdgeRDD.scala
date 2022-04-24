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

  def mapToFile(filePrefix : String, mappedSize : Long) : Array[String]
}

object GrapeEdgeRDD {
  def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]): GrapeEdgeRDDImpl[ED] = {
    //Shuffle the edge rdd
    //then use build to build.
    null
  }

  def fromEdgePartitions[ED: ClassTag](
        grapeEdgePartitions: RDD[(Int, GrapeEdgePartition[ED])]): GrapeEdgeRDDImpl[ED] = {
    new GrapeEdgeRDDImpl[ED](grapeEdgePartitions)
  }
}
