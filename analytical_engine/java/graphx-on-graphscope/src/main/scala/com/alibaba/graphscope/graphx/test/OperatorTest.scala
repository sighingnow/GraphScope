package com.alibaba.graphscope.graphx.test

import com.alibaba.graphscope.graphx.GraphScopeHelper
import org.apache.spark.graphx.{EdgeTriplet, Graph, GraphLoader, PartitionID, TripletFields}
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
      sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      val rawGraph = GraphLoader.edgeListFile(sc, fileName,false, partNum)
      val graph = rawGraph.mapVertices((vid,vd)=>vd.toLong).mapEdges(edge=>edge.attr.toLong)
      val grapeGraph = GraphScopeHelper.graph2Fragment[Long,Long](graph)

      val maskGraph : Graph[Long,Long] = graph.subgraph(epred = (_ => true), vpred = (id, vd) => id % 2 == 0)
      val grapeMaskGraph : Graph[Long,Long] = grapeGraph.subgraph(epred = (_ => true), vpred = (id, _)=>id % 2 == 0)

      def mapping(graph : Graph[Long,Long])  : Graph[Long,Long] = {
        log.info("[Operator test]: start Mapping")
        graph.mapVertices((vid, vd) => vd + vid)
          .mapEdges(edge=> edge.srcId + edge.dstId + edge.attr)
          .mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr + triplet.attr + triplet.srcId + triplet.dstId)
      }

      def mapDifferentType(graph : Graph[Long,Long]) : Graph[Long,Long] = {
        log.info("[Operator test]: start Mapping different type")
        val tmp = graph.mapVertices((vid, vd) => vd.toDouble).mapEdges(edge=> edge.attr.toDouble)
        tmp.mapVertices((vid, vd) => vd.toLong).mapEdges(edge=> edge.attr.toLong)
      }

      def outerJoin(graph : Graph[Long,Long]) : Graph[Long,Long] = {
        log.info("[Operator test]: start outer join")
        val inDegrees = graph.inDegrees
        graph.joinVertices(inDegrees)((id, ovd, newVd) => {
//          log.info(s"vertex ${id}, set vd from ${ovd} to ${newVd}")
          newVd
        })
      }

      def subGraph(graph: Graph[Long,Long]) : Graph[Long,Long] = {
        log.info("[Operator test]: start subgraph")
        graph.subgraph(epred = { triplet => triplet.srcId > 100}, vpred = (vid,vd) => vid > 100)
      }

      //map edge attr to pid.
      def mapEdgeIterator(graph : Graph[Long,Long]) : Graph[Long,Long] = {
        log.info("[Operator test]: start map edges")
        graph.mapEdges((pid,iter)=> iter.map(e=>e.srcId))
      }

      def mapTriplet(graph : Graph[Long,Long]) : Graph[Long,Long] = {
        log.info("[Operator test]: start map triplets")
        val graph2 = graph.mapTriplets(triplet => {
//          log.info(s"triplet v1 ${triplet.srcId}(${triplet.srcAttr})->${triplet.dstId}(${triplet.dstAttr}), attr ${triplet.attr}")
          triplet.srcAttr + triplet.dstAttr
        })
        def f(pid : PartitionID, iter : Iterator[EdgeTriplet[Long,Long]]): Iterator[Long] = {
          iter.map(triplet=> {
//            log.info(s"triplet v2 ${triplet.srcId}(${triplet.srcAttr})->${triplet.dstId}(${triplet.dstAttr}), attr ${triplet.attr}")
            triplet.srcId
          })
        }
        graph2.mapTriplets[Long]((pid,iter)=>f(pid,iter), TripletFields.All)
      }

//      val graphxRes = mapTriplet(mapEdgeIterator(mapEdgeIterator(subGraph(outerJoin(mapDifferentType(mapping(graph))))))).mask(maskGraph).reverse
//      val graphxRes = mapTriplet(mapEdgeIterator(subGraph(outerJoin(mapDifferentType(mapping(graph)))))).mask(maskGraph)
      val graphxRes = mapTriplet(graph)
      val grapeRes = mapTriplet(grapeGraph)

      log.info(s"after map triplets vertices ${grapeRes.vertices.count()}, edges ${grapeRes.edges.count()}")
      log.info(s"after map triplets vertices ${graphxRes.vertices.count()}, edges ${graphxRes.edges.count()}")

      val graphxRes2 = mapDifferentType(mapping(graph))
      val grapeRes2 = mapDifferentType(mapping(grapeGraph))

      log.info(s"after map, vertices ${grapeRes2.vertices.count()}, edges ${grapeRes2.edges.count()}")
      log.info(s"after map, vertices ${graphxRes2.vertices.count()}, edges ${graphxRes2.edges.count()}")

      val graphxRes3 = outerJoin(graph)
      val grapeRes3 = outerJoin(grapeGraph)

      log.info(s"after outer join vertices ${grapeRes3.vertices.count()}, edges ${grapeRes3.edges.count()}")
      log.info(s"after outer join vertices ${graphxRes3.vertices.count()}, edges ${graphxRes3.edges.count()}")


      //      val grapeRes = mapTriplet(mapEdgeIterator(subGraph(outerJoin(mapDifferentType(mapping(grapeGraph)))))).mask(grapeMaskGraph)

      graphxRes3.vertices.saveAsTextFile(s"/tmp/operator-test-graphx-vertex-${java.time.LocalDateTime.now()}")
      grapeRes3.vertices.saveAsTextFile(s"/tmp/operator-test-grape-vertex-${java.time.LocalDateTime.now()}")
      graphxRes.edges.saveAsTextFile(s"/tmp/operator-test-graphx-edge-${java.time.LocalDateTime.now()}")
      grapeRes.edges.saveAsTextFile(s"/tmp/operator-test-grape-edge-${java.time.LocalDateTime.now()}")
      sc.stop()
    }
  }

