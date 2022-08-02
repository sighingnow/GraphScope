package com.alibaba.graphscope.example.graphx

import com.alibaba.graphscope.graphx.GraphScopeHelper
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object PageRank extends Logging{
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
    val eFilePath = args(0);
    val numPartitions = args(1).toInt;
    log.info(s"Running for efile ${eFilePath}")
    val graph = GraphScopeHelper.edgeListFile(sc, eFilePath,canonicalOrientation = false,numPartitions)
    graph.cache()
    log.info(s"[PageRank: ] Load graph ${graph.numEdges}, ${graph.numVertices}")
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    // Print the result

    log.info(s"Finish query, graph vertices: ${graph.numVertices}  and edges: ${graph.numEdges}")

    sc.stop()
  }
}
