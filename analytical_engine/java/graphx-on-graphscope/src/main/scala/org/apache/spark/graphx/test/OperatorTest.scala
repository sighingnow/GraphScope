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
          .mapEdges(edge=>edge.srcId + edge.dstId + edge.attr)
          .mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr + triplet.attr + triplet.srcId + triplet.dstId)
      }

      def mapDifferentType(graph : Graph[Long,Long]) : Graph[Double,Double] = {
        graph.mapVertices((vid, vd) => vd.toDouble).mapEdges(edge=>edge.attr.toDouble)
      }

      val graphxRes = mapDifferentType(mapping(graph))

      val grapeRes = mapDifferentType(mapping(grapeGraph))

      graphxRes.vertices.saveAsTextFile(s"/tmp/operator-test-${java.time.LocalDateTime.now()}-graphx-vertex")
      grapeRes.vertices.saveAsTextFile(s"/tmp/operator-test-${java.time.LocalDateTime.now()}-grape-vertex")
      graphxRes.edges.saveAsTextFile(s"/tmp/operator-test-${java.time.LocalDateTime.now()}-graphx-edge")
      grapeRes.edges.saveAsTextFile(s"/tmp/operator-test-${java.time.LocalDateTime.now()}-grape-edge")
      sc.stop()
    }
  }

