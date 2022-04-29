package org.apache.spark.graphx

import org.apache.spark.graphx.impl.GrapeEdgePartition
import org.apache.spark.graphx.impl.offheap.OffHeapEdgeRDDImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import scala.reflect.ClassTag

abstract class GrapeEdgeRDD[ED](sc: SparkContext,
                                deps: Seq[Dependency[_]]) extends EdgeRDD[ED](sc, deps) {
  def mapValues[ED2 : ClassTag](f: Edge[ED] => ED2): GrapeEdgeRDD[ED2]
  override def innerJoin[ED2: ClassTag, ED3: ClassTag](other: EdgeRDD[ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgeRDD[ED3]
}

object GrapeEdgeRDD {
  def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]): GrapeEdgeRDD[ED] = {
    //Shuffle the edge rdd
    //then use build to build.
    null
  }

  private[graphx] def fromEdgePartitions[VD: ClassTag, ED : ClassTag](
                                                        edgePartitions: RDD[(PartitionID, GrapeEdgePartition[VD, ED])]): OffHeapEdgeRDDImpl[VD, ED] = {
    //    new EdgeRDDImpl(edgePartitions)
    new OffHeapEdgeRDDImpl[VD,ED](edgePartitions)
  }
}
