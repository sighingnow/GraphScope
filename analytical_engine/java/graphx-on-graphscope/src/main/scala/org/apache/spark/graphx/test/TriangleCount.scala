package org.apache.spark.graphx.test

import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object TriangleCount extends Logging{
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 2) {
      println("Expect 1 args")
      return 0;
    }
    val efile = args(0)
    val partNum = args(1).toInt
    val graph: Graph[Int, Int] =
      GraphLoader.edgeListFile(sc, efile,canonicalOrientation = false, partNum)
    // Initialize the graph such that all vertices except the root have distance infinity.
    val triCounts = graph.triangleCount().vertices
    triCounts.saveAsTextFile(s"/tmp/triCounts-${java.time.LocalDateTime.now()}")
  }
}
