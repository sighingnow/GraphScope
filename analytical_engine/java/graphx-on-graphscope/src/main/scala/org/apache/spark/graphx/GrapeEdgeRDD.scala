package org.apache.spark.graphx

import org.apache.spark.graphx.impl.{EdgePartition, EdgeRDDImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import scala.reflect.ClassTag

abstract class GrapeEdgeRDD[ED](sc: SparkContext,
                                deps: Seq[Dependency[_]]) extends EdgeRDD[ED](sc, deps) {
}

object GrapeEdgeRDD {
  def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]): GrapeEdgeRDD[ED] = {
    //Shuffle the edge rdd
    //then use build to build.
    null
  }

  private[graphx] def fromEdgePartitions[ED: ClassTag, VD: ClassTag](
                                                                      edgePartitions: RDD[(Int, EdgePartition[ED, VD])]): EdgeRDDImpl[ED, VD] = {
    new EdgeRDDImpl(edgePartitions)
  }
}
