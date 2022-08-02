package com.alibaba.graphscope.graphx.test

import com.alibaba.graphscope.graphx.rdd.impl.VertexDataMessage
import com.alibaba.graphscope.graphx.{GraphScopeHelper, VertexData}
import com.alibaba.graphscope.graphx.shuffle.EdgeShuffle
import com.alibaba.graphscope.graphx.utils.DoubleDouble
import org.apache.spark.graphx.{Graph, GraphLoader, VertexId}
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
    sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc.getConf.registerKryoClasses(Array(classOf[EdgeShuffle[_,_]], classOf[Array[Long]], classOf[Array[Int]], classOf[VertexDataMessage[_]], classOf[DoubleDouble]))
    if (args.length < 3) {
      println("Expect 4 args")
      return 0;
    }
    val efile = args(0)
    val partNum = args(1).toInt
    val engine = args(2)
    val time0 = System.nanoTime()
    val graph: Graph[Int, Double] = {
      if (engine.equals("gs")) {
        GraphScopeHelper.edgeListFile(sc, efile, canonicalOrientation = false, partNum).mapEdges(e => e.attr.toDouble)
      }
      else if (engine.equals("graphx")){
        GraphLoader.edgeListFile(sc, efile,canonicalOrientation = false, partNum).mapEdges(e => e.attr.toDouble)
      }
      else {
        throw new IllegalStateException("gs or graphx")
      }
    }
    // Initialize the graph such that all vertices except the root have distance infinity.
    log.info(s"initial graph count ${graph.numVertices}, ${graph.numEdges}")
    val time1 = System.nanoTime()

    val pagerank = graph.pageRank(0.001)
    log.info(s"pagerank graph count ${pagerank.numVertices}, ${pagerank.numEdges}")
    val time2 = System.nanoTime()
    log.info(s"Pregel took ${(time2 - time1)/1000000}ms, load graph ${(time1 - time0)/1000000}ms")

  }
}
