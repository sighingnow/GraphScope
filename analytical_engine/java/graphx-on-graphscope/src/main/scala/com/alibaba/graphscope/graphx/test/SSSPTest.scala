package com.alibaba.graphscope.graphx.test

import com.alibaba.graphscope.graphx.rdd.impl.VertexDataMessage
import com.alibaba.graphscope.graphx.{GraphScopeHelper, VertexData}
import com.alibaba.graphscope.graphx.shuffle.EdgeShuffle
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
    sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc.getConf.registerKryoClasses(Array(classOf[EdgeShuffle[_,_]], classOf[Array[Long]], classOf[Array[Int]], classOf[VertexDataMessage[_]]))
    if (args.length < 4) {
      println("Expect 4 args")
      return 0;
    }
    val efile = args(0)
    val partNum = args(1).toInt
    val sourceId = args(2).toLong
    val engine = args(3)
    val time0 = System.nanoTime()
    val graph: Graph[Int, Double] = {
      if (engine.equals("gs")) {
        //      GraphLoader.edgeListFile(sc, efile,canonicalOrientation = false, partNum).mapEdges(e => e.attr.toDouble)
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
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity).cache()
    log.info(s"initial graph count ${initialGraph.numVertices}, ${initialGraph.numEdges}")
    val time1 = System.nanoTime()

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) =>{
//        log.info(s"vertex ${id} receive msg ${newDist}, original ${dist}")
          math.min(dist, newDist); // Vertex Program
      },
      triplet => {  // Send Message
//        log.info(s"visting triplet ${triplet.srcId}(${triplet.srcAttr})->${triplet.dstId}(${triplet.dstAttr}), ${triplet.attr}")
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    log.info(s"initial graph count ${sssp.numVertices}, ${sssp.numEdges}")
    val time2 = System.nanoTime()
    log.info(s"Pregel took ${(time2 - time1)/1000000}ms, load graph ${(time1 - time0)/1000000}ms")

  }
}
