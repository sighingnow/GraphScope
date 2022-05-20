package com.alibaba.graphscope.example.graphx

import org.apache.spark.graphx.{EdgeTriplet, GraphLoader, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class GraphTransform extends Logging{
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 2){
      println("Expect 3 args")
      return 0;
    }
    val efilePath = args(0)
    val numPartition = args(1).toInt
    log.info(s"efile path ${efilePath}, numPartition ${numPartition}")
    val loadGraph0 = System.nanoTime();
    val graph = GraphLoader.edgeListFile(sc, efilePath,canonicalOrientation = false,numPartition).cache()
    val loadGraph1 = System.nanoTime();
    log.info(s"[GraphLoader: ] Load graph ${graph.numEdges}, ${graph.numVertices}")
    // Initialize the graph such that all vertices except the root have distance infinity.
    println(s"Graph has ${graph.numVertices} vertices, ${graph.numEdges} edges")

    val startTime = System.nanoTime();
    val graph1 = graph.mapVertices((vid, vd)=> vid)
    val graph2 = graph1.mapVertices((vid, vd)=> vd.toDouble)
    val graph3 = graph2.outDegrees.mapValues(_ => 0)
    val degreeSum = graph3.values.sum()
    log.info(s"after filter ${degreeSum}")
    val graph5 = graph.groupEdges((a,b) => a +b)
    val inDegreeSum = graph5.inDegrees.values.sum()
    val outDegreeSum = graph5.outDegrees.values.sum()
    log.info(s"in degree sum ${inDegreeSum}, out degree sum ${outDegreeSum}")

    val graph6 = graph.mapEdges(edge => edge.srcId)
    val triplets = graph6.triplets.collect()
    for (triplet <- triplets){
      log.info(s"triplet: ${triplet}")
    }

    val graph7 = graph.reverse
    val graph8 = graph7.filter(_.mapVertices((_, vd)=> vd.toInt).mapEdges(edge=>edge.attr), epred = (trip : EdgeTriplet[Int,Int]) => true, vpred = (vid:VertexId, vd: Int) => vid== 1)
    graph8.numVertices
    val endTime = System.nanoTime()
    println("[Query time ] : " + ((endTime - startTime) / 1000000) + "ms")
    println("[Load graph time ]: " + ((loadGraph1 - loadGraph0) / 1000000) + "ms")
    sc.stop()
  }

}
