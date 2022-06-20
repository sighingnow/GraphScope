package org.apache.spark.graphx.test

import com.alibaba.graphscope.graphx.{GSSession, GraphScopeHelper}
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object TraverseTest extends Logging{
  def main(array: Array[String]) : Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    val gsSession: GSSession = GraphScopeHelper.createSession(sc)

    //val graph: GrapeGraphImpl[Long, Long] =
    //  GraphScopeRDD.loadFragmentAsGraph[Long, Long](sc,
    //    "d50:250522961160603508",
    //    "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>")

    val graph : Graph[Long,Long] = GraphLoader.edgeListFile(sc, "/home/graphscope/data/lei.e", false, numEdgePartitions = 1).mapVertices((id,vd)=>vd.toLong).mapEdges(edge=>edge.attr.toLong).cache()
    val graph2 = graph.mapVertices((id,vd) => (vd,vd))
    val graph3 = graph2.mapEdges(edge => edge.attr + 10L)
    val res = graph3.pregel(1L, maxIterations = 10)(
      (id, dist, newDist) => (newDist,newDist),
      triplet => { // Send Message
        log.info(s"visiting triplet ${triplet.srcId}(${triplet.srcAttr}) -> ${triplet.dstId}(${triplet.dstAttr}), attr ${triplet.attr}")
        Iterator.empty
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    gsSession.close()
    sc.stop()
  }
}
