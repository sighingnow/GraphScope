package org.apache.spark.graphx.test

import org.apache.spark.graphx.{Graph, GraphLoader, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object BFSTest extends Logging{
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
    val graph: Graph[Int, Int] =
      GraphLoader.edgeListFile(sc, efile,canonicalOrientation = false, partNum)
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0 else Int.MaxValue)
    val sssp = initialGraph.pregel(Int.MaxValue, 100)(
      (id, dist, newDist) =>{
        math.min(dist, newDist); // Vertex Program
      },
      triplet => {  // Send Message
        if (triplet.srcAttr < triplet.dstAttr - 1) {
          Iterator((triplet.dstId, triplet.srcAttr + 1))
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
