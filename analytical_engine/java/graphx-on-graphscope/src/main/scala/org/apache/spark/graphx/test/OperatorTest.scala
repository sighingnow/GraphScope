package org.apache.spark.graphx.test

import com.alibaba.graphscope.graphx.GraphScopeHelper
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.graphx.rdd.GraphScopeRDD
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object OperatorTest extends Logging{
    def main(array: Array[String]) : Unit = {
      require(array.length == 2)
      val fileName = array(0)
      val partNum = array(1).toInt
      val spark = SparkSession
        .builder
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()
      val sc = spark.sparkContext

      val rawGraph = GraphLoader.edgeListFile(sc, fileName,true, partNum)
      val graph = rawGraph.mapVertices((vid,vd)=>vd.toLong).mapEdges(edge=>edge.attr.toLong)
      val grapeGraph = GraphScopeHelper.graph2Fragment[Long,Long](graph)

      def mapping(graph : Graph[Long,Long])  : Graph[Long,Long] = {
        graph.mapVertices((vid, vd) => vd + vid)
          .mapEdges(edge=> {log.info(s"edge ${edge.srcId}->${edge.dstId}:${edge.attr}");edge.srcId + edge.dstId + edge.attr})
//          .mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr + triplet.attr + triplet.srcId + triplet.dstId)
      }

      def mapDifferentType(graph : Graph[Long,Long]) : Graph[Double,Double] = {
        graph.mapVertices((vid, vd) => {log.info(s"${vid}(${vd})"); vd.toDouble}).mapEdges(edge=>{log.info(s"edge attr ${edge.attr}"); edge.attr.toDouble})
      }

      val graphxRes = mapDifferentType(mapping(graph))

      val grapeRes = mapDifferentType(mapping(grapeGraph))

      graphxRes.vertices.saveAsTextFile(s"/tmp/operator-test-graphx-vertex-${java.time.LocalDateTime.now()}")
      grapeRes.vertices.saveAsTextFile(s"/tmp/operator-test-grape-vertex-${java.time.LocalDateTime.now()}")
      graphxRes.edges.saveAsTextFile(s"/tmp/operator-test-graphx-edge-${java.time.LocalDateTime.now()}")
      grapeRes.edges.saveAsTextFile(s"/tmp/operator-test-grape-edge-${java.time.LocalDateTime.now()}")
      sc.stop()
    }
  }

