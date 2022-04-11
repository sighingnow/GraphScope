package com.alibaba.graphscope.example

import org.apache.spark.graphx.{Graph, GraphLoader, VertexId}
import org.apache.spark.sql.SparkSession

object SSSP {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // A graph with edge attributes containing distances
    val graph: Graph[Long, Double] = {
    GraphLoader.edgeListFile(sc, "/home/graphscope/data/livejournal.e", false, 2)
      .mapEdges(e => e.attr.toDouble).mapVertices((vid, _) => vid)
    }
    ///home/graphscope/data/gstest/p2p-31.e
    val sourceId: VertexId = 1 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)
//    println(initialGraph.vertices.collect().mkString("Array(", ", ", ")"))
//    println(initialGraph.edges.collect().mkString("Array(", ", ", ")"))
    val sssp = initialGraph.pregel(Double.PositiveInfinity)( //avoid overflow
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
//          println(triplet.srcAttr + ", " + (triplet.srcAttr + triplet.attr) + ", " + triplet.dstAttr)
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
//    println(sssp.vertices.collect.mkString("\n"))
    // $example off$

    sc.stop()
  }
}
