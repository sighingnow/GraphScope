package com.alibaba.graphscope.example

import com.alibaba.graphscope.app.GraphXAppBase
import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

/**
 * vd = long,
 * ed = long,
 * msg_t = long
 */
class SSSP extends GraphXAppBase[Long,Long,Long]{
  override def vprog(): (VertexId, Long, Long) => Long = {
    (id, dist, newDist) => math.min(dist, newDist)
  }

  override def sendMsg(): EdgeTriplet[Long, Long] => Iterator[(VertexId, Long)] = {
    triplet => {  // Send Message
      if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
        Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
      } else {
        Iterator.empty
      }
    }
  }

  override def mergeMsg(): (Long, Long) => Long = {
    (a, b) => math.min(a, b)
  }
}
object SSSP{
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val sparkHome=System.getenv("SPARK_HOME")
    if (sparkHome == null || sparkHome.isEmpty){
      System.out.println("SPARK_HOME not available")
      return ;
    }
    val user_jar_path = System.getenv("USER_JAR_PATH")
    if (user_jar_path == null || user_jar_path.isEmpty){
      System.out.println("USER_JAR_PATH not set")
      return ;
    }
    val jars = new Array[String](1)
    jars(0) = user_jar_path
    var sparkContext = new SparkContext("local","SSSP", sparkHome, jars);

    // $example on$
    // A graph with edge attributes containing distances
    val graph: Graph[Long, Double] =
    GraphGenerators.logNormalGraph(sparkContext, numVertices = 100).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 42 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
    // $example off$

    sparkContext.stop()
  }
}
