package com.alibaba.graphscope.graphx.test

import com.alibaba.graphscope.graphx.GraphScopeHelper
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object PageRankTest extends Logging{
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
//    val graph = GraphScopeHelper.edgeListFile(sc, eFilePath,canonicalOrientation = false, numPartitions).mapVertices((vid,vd)=>vd.toLong).mapEdges(edge=>edge.attr).cache()
    val graph = GraphLoader.edgeListFile(sc, eFilePath,canonicalOrientation = false, numPartitions).mapVertices((vid,vd)=>vd.toLong).mapEdges(edge=>edge.attr).cache()
    log.info(s"[PageRank: ] Load graph ${graph.numEdges}, ${graph.numVertices}")
    val ranks = graph.pageRank(0.0001).vertices

    log.info(s"Finish query, graph vertices: ${graph.numVertices}  and edges: ${graph.numEdges}")
    ranks.saveAsTextFile(s"/tmp/pagerank-test-${java.time.LocalDateTime.now()}")

    sc.stop()
  }
}

