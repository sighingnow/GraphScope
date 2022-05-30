package org.apache.spark.graphx.test

import org.apache.spark.graphx.rdd.GraphScopeRDD
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object FragmentAsRDDTest extends Logging{
  def main(array: Array[String]) : Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    require(array.length == 2)
    val objectID = array(0).toLong
    val fragName = array(1)
    log.info(s"Getting fragment ${objectID} as RDD, frag type ${fragName}")
    val (vertexRDD,edgeRDD) = GraphScopeRDD.loadFragmentAsRDD(sc, objectID, fragName)
    log.info(s"vertices count ${vertexRDD.count()}, edge cout ${edgeRDD.count()}")
    sc.stop()
  }
}
