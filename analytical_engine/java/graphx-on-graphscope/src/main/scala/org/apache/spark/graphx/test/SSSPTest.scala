package org.apache.spark.graphx.test

import org.apache.spark.graphx.{Graph, GraphLoader, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SSSPTest extends Logging{
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 3) {
      println("Expect 1 args")
      return 0;
    }
    val efile = args(0)
    val partNum = args(1).toInt
    val sourceId = args(2).toLong
    val graph: Graph[Int, Double] =
      GraphLoader.edgeListFile(sc, efile,canonicalOrientation = false, partNum).mapEdges(e => e.attr.toDouble)
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity, 100)(
      (id, dist, newDist) =>{
//        log.info(s"vertex ${id} receive msg ${newDist}, original ${dist}")
          math.min(dist, newDist); // Vertex Program
      },
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
//    println(sssp.vertices.collect.mkString("\n"))
    sssp.vertices.saveAsTextFile(s"/tmp/sssp-test-${java.time.LocalDateTime.now()}")
    // $example off$

  }
}
