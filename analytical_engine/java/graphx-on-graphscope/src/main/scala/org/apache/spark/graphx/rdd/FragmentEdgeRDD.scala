package org.apache.spark.graphx.rdd

import org.apache.spark.graphx._
import org.apache.spark.graphx.rdd.impl.FragmentEdgePartition
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import scala.reflect.ClassTag

abstract class FragmentEdgeRDD[ED](sc: SparkContext,
                                deps: Seq[Dependency[_]]) extends EdgeRDD[ED](sc, deps) {

  private[graphx] def grapePartitionsRDD: RDD[FragmentEdgePartition[VD, ED]] forSome { type VD }

  override def partitionsRDD = null

  def mapValues[ED2 : ClassTag](f: Edge[ED] => ED2): FragmentEdgeRDD[ED2]

  override def innerJoin[ED2: ClassTag, ED3: ClassTag](other: EdgeRDD[ED2])
                                                      (f: (VertexId, VertexId, ED, ED2) => ED3): FragmentEdgeRDD[ED3]

  override def reverse: FragmentEdgeRDD[ED];

  def generateDegreeRDD(originalVertexRDD : FragmentVertexRDD[_], edgeDirection : EdgeDirection) : FragmentVertexRDD[Int]
}