package com.alibaba.graphscope.graphx.test

// $example on$
import com.alibaba.graphscope.graphx.GraphScopeHelper
import com.alibaba.graphscope.graphx.rdd.impl.VertexDataMessage
import com.alibaba.graphscope.graphx.shuffle.EdgeShuffle
import com.alibaba.graphscope.graphx.test.PageRankTest.log
import com.alibaba.graphscope.graphx.utils.DoubleDouble
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * A connected components algorithm example.
 * The connected components algorithm labels each connected component of the graph
 * with the ID of its lowest-numbered vertex.
 * For example, in a social network, connected components can approximate clusters.
 * GraphX contains an implementation of the algorithm in the
 * [`ConnectedComponents` object][ConnectedComponents],
 * and we compute the connected components of the example social network dataset.
 *
 * Run with
 * {{{
 * bin/run-example graphx.ConnectedComponentsExample
 * }}}
 */
object CCTest extends Logging{
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

    val cc = graph.connectedComponents()
    log.info(s"cc graph count ${cc.numVertices}, ${cc.numEdges}")
    val time2 = System.nanoTime()
    log.info(s"Pregel took ${(time2 - time1)/1000000}ms, load graph ${(time1 - time0)/1000000}ms")
  }
}

