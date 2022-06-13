package org.apache.spark.graphx.test

import com.alibaba.graphscope.graphx.{GSSession, GraphScopeHelper}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object GraphConvertTest extends Logging{
  def main(array: Array[String]) : Unit = {
    require(array.length == 2)
    val path = array(0)
    val numPart = array(1).toInt
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    val gsSession: GSSession = GraphScopeHelper.createSession(sc)

    val graphxGraph = GraphLoader.edgeListFile(sc, path,false, numPart).mapVertices((id,vd)=> id)
    log.info(s"Loaded graphx graph ${graphxGraph.numVertices} vertices, ${graphxGraph.numEdges} edges")

    val grapeGraph = GraphScopeHelper.graph2Fragment(graphxGraph)
    log.info(s"Converted to grape graph ${grapeGraph.numVertices} vertices, ${grapeGraph.numEdges} edges")
    val res = grapeGraph.mapVertices((id,vd) => {log.info(s"${id}: ${vd}"); vd})
    log.info(s"Converted to grape graph ${res.numVertices} vertices, ${res.numEdges} edges")

    //FIXME: Run GraphScope SSSP on this graph

    gsSession.close()
    sc.stop()
  }
}
