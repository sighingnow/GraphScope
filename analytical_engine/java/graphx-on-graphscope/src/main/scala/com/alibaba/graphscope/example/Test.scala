package com.alibaba.graphscope.example

import com.alibaba.graphscope.graphx.GraphLoader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object Test extends Logging {
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
    val graph = GraphLoader.edgeListFile[Long, Long](sc, eFilePath, 1L, false, numPartitions)
    val mapped_graph = graph.mapVertices((vid, vdata) => 99999L)
    mapped_graph.cache()
    log.info(s"Finish loading, graph vertices: ${graph.numVertices}  and edges: ${graph.numEdges}")

    val res = mapped_graph.pregel(99999L, maxIterations = 100)(
      (vid, vd, msg) => {
        math.min(vd,msg)
      },
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => a
    )
    log.info(s"Finish query, graph vertices: ${res.numVertices}  and edges: ${res.numEdges}")
    log.info(s"${res.vertices.collect().mkString("Array(", ", ", ")")}")
  }
}
