package com.alibaba.graphscope.example.graphx

import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object SSSP extends Logging{
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 3){
      println("Expect 3 args")
      return 0;
    }
    val efilePath = args(0)
    val numParition = args(1).toInt
    val sourceId: VertexId = args(2).toLong // The ultimate source
    log.info(s"efile path ${efilePath}, numPartition ${numParition}, sourceId ${sourceId}")
    val graph = GraphLoader.edgeListFile(sc, efilePath,canonicalOrientation = false,numParition)
    log.info(s"[GraphLoader: ] Load graph ${graph.numEdges}, ${graph.numVertices}")
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, vdata) =>
      if (id == sourceId) 0.0 else vdata.toDouble)
//    println(initialGraph.vertices.collect().mkString("Array(", ", ", ")"))
//    println(initialGraph.edges.collect().mkString("Array(", ", ", ")"))
    val edgesNum = initialGraph.numEdges
    val verticesNum = initialGraph.numVertices
    println(s"Graph has ${verticesNum} vertices, ${edgesNum} edges")

    val startTime = System.nanoTime();
    println("[Start pregel]")
    val sssp = initialGraph.pregel(Double.PositiveInfinity, 10)( //avoid overflow
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        println(triplet.srcId + ", to  " + triplet.dstId + ", data "+ (triplet.srcAttr + triplet.attr) + ", " + triplet.dstAttr)
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    val endTIme = System.nanoTime()
    println("[Pregel running time ] : " + ((endTIme - startTime) / 1000000) + "ms")
    sc.stop()
  }
}
