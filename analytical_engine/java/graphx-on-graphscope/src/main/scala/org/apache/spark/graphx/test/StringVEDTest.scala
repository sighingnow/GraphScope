package org.apache.spark.graphx.test

import com.alibaba.graphscope.graphx.{GSSession, GraphScopeHelper}
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.graphx.test.TraverseTest.log
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object StringVEDTest extends Logging{
  def main(args: Array[String]) : Unit = {
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

    val graph : Graph[Long,Long] = GraphLoader.edgeListFile(sc, eFilePath, false, numEdgePartitions = numPartitions).mapVertices((id,vd)=>vd.toLong).mapEdges(edge=>edge.attr.toLong).cache()
    val graph2 = graph.mapVertices((id,vd) => id).mapVertices((id,vd)=>(vd,vd))
    val graph3 = graph2.mapEdges(edge => (edge.srcId,edge.dstId))
    val res = graph3.pregel((1L,1L), maxIterations = 10)(
      (id, dist, newDist) => {
        log.info(s"visiting vertex ${id}(${dist}), new dist${newDist}")
        newDist
      },
      triplet => { // Send Message
        log.info(s"visiting triplet ${triplet.srcId}(${triplet.srcAttr}) -> ${triplet.dstId}(${triplet.dstAttr}), attr ${triplet.attr}")
        Iterator.empty
      },
      (a, b) => (Math.min(a._1,b._1),Math.max(a._2,b._2)) // Merge Message
    )
    sc.stop()
  }
}
