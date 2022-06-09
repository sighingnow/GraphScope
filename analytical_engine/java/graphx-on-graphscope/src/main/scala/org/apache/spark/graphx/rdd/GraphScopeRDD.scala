package org.apache.spark.graphx.rdd

import org.apache.spark.SparkContext
import org.apache.spark.graphx.impl.GrapeGraphImpl
import org.apache.spark.graphx.scheduler.cluster.ExecutorInfoHelper
import org.apache.spark.graphx.{GrapeEdgeRDD, GrapeVertexRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{ParallelCollectionRDD, RDD}

import scala.reflect.ClassTag

/**
 * Provide utils to interact with GraphScope.
 */
object GraphScopeRDD extends Logging{

  def loadFragmentAsRDD[VD: ClassTag, ED: ClassTag](sc : SparkContext, objectIDs : String, fragName : String) : (GrapeVertexRDD[VD],GrapeEdgeRDD[ED]) = {

    val fragmentRDD = new FragmentRDD[VD,ED](sc, ExecutorInfoHelper.getExecutors(sc), fragName,objectIDs)
    fragmentRDD.generateRDD()
  }

  def loadFragmentAsGraph[VD: ClassTag, ED: ClassTag](sc : SparkContext, objectIDs : String, fragName : String) : GrapeGraphImpl[VD,ED] = {
    val (vertexRDD,edgeRDD) = loadFragmentAsRDD[VD,ED](sc, objectIDs, fragName);
    GrapeGraphImpl.fromExistingRDDs[VD,ED](vertexRDD,edgeRDD)
  }

  def makeRDD(sc : SparkContext, seq: Array[(Long, Array[String])]): RDD[Long] ={
    val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2.toSeq)).toMap
    new ParallelCollectionRDD[Long](sc, seq.map(_._1), math.max(seq.size, 1), indexToPrefs)
  }

}
