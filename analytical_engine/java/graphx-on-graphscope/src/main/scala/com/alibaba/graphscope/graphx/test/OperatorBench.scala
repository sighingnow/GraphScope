package com.alibaba.graphscope.graphx.test

import com.alibaba.graphscope.graphx.GraphScopeHelper
import org.apache.spark.graphx.{EdgeTriplet, Graph, GraphLoader, PartitionID, TripletFields}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object OperatorBench extends Logging{
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
    val graphxGraph = rawGraph.mapVertices((vid,vd)=>vd.toLong).mapEdges(edge=>edge.attr.toLong)
    val grapeGraph = GraphScopeHelper.graph2Fragment[Long,Long](graphxGraph).cache()

    val maskGraph : Graph[Long,Long] = graphxGraph.subgraph(epred = (_ => true), vpred = (id, vd) => id % 2 == 0)
    val grapeMaskGraph : Graph[Long,Long] = grapeGraph.subgraph(epred = (_ => true), vpred = (id, _)=>id % 2 == 0)

    def mapping(graph : Graph[Long,Long])  : Graph[Long,Long] = {
      graph.mapVertices((vid, vd) => vd + vid)
//        .mapEdges(edge=> edge.srcId + edge.dstId + edge.attr)
//        .mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr + triplet.attr + triplet.srcId + triplet.dstId)
    }

    def mapDifferentType(graph : Graph[Long,Long]) : Graph[Long,Long] = {
      val tmp = graph.mapVertices((vid, vd) => vd.toDouble) //.mapEdges(edge=> edge.attr.toDouble)
      tmp.mapVertices((vid, vd) => vd.toLong) //.mapEdges(edge=> edge.attr.toLong)
    }

    def outerJoin(graph : Graph[Long,Long]) : Graph[Long,Long] = {
      val inDegrees = graph.inDegrees
      graph.joinVertices(inDegrees)((id, ovd, newVd) => {
        newVd
      })
    }

    def subGraph(graph: Graph[Long,Long]) : Graph[Long,Long] = {
      graph.subgraph(epred = { triplet => triplet.srcId > 100}, vpred = (vid,vd) => vid > 100)
    }

    //map edge attr to pid.
    def mapEdgeIterator(graph : Graph[Long,Long]) : Graph[Long,Long] = {
      graph.mapEdges((pid,iter)=> iter.map(e=>e.srcId))
    }

    def mapEdges(graph : Graph[Long,Long]) : Graph[Long,Long] = {
      graph.mapEdges(edge => edge.dstId + edge.srcId)
    }

    def mapTriplet(graph : Graph[Long,Long]) : Graph[Long,Long] = {
      graph.mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr)
    }

    def mapTripletIterator(graph : Graph[Long,Long]) : Graph[Long,Long] = {
      def f(pid : PartitionID, iter : Iterator[EdgeTriplet[Long,Long]]): Iterator[Long] = {
        iter.map(triplet=> triplet.srcId)
      }
      graph.mapTriplets[Long]((pid,iter)=>f(pid,iter), TripletFields.All)
    }

    //0. mapping vertices

    val grapeTime10 = System.nanoTime()
    val grapeGraph1 = mapDifferentType(mapDifferentType(mapDifferentType(mapping(mapping(mapping(grapeGraph))))))
    log.info(s"Finish mapping grape vertices ${grapeGraph1.vertices.count()}")
    val grapeTime11 = System.nanoTime()
    val graphxTime10 = System.nanoTime()
    val graphxGraph1 = mapDifferentType(mapDifferentType(mapDifferentType(mapping(mapping(mapping(graphxGraph))))))
    log.info(s"Finish mapping graphx vertices ${graphxGraph1.vertices.count()}")
    val graphxTime11 = System.nanoTime()
    graphxGraph1.unpersist()
    grapeGraph1.unpersist()

    //1. mapping edges

    val grapeTime20 = System.nanoTime()
    val grapeGraph2 = mapEdges(mapEdges(mapEdges(grapeGraph)))
    log.info(s"Finish mapping grape edge, counts vertices ${grapeGraph2.vertices.count()}, edges ${grapeGraph2.edges.count()}")
    val grapeTime21 = System.nanoTime()
    val graphxTime20 = System.nanoTime()
    val graphxGraph2 = mapEdges(mapEdges(mapEdges(graphxGraph)))
    log.info(s"Finish mapping graphx edge, counts vertices ${graphxGraph2.vertices.count()} edges ${graphxGraph2.edges.count()}")
    val graphxTime21 = System.nanoTime()
    graphxGraph2.unpersist()
    grapeGraph2.unpersist()

    //1.join
    val grapeTime30 = System.nanoTime()
    val grapeGraph3 = outerJoin(outerJoin(outerJoin(grapeGraph)))
    log.info(s"Finish join, counts vertices ${grapeGraph3.vertices.count()}, edges ${grapeGraph3.edges.count()}")
    val grapeTime31 = System.nanoTime()
    val graphxTime30 = System.nanoTime()
    val graphxGraph3 = outerJoin(outerJoin(outerJoin(graphxGraph)))
    log.info(s"Finish join, counts vertices ${graphxGraph3.vertices.count()} edges ${graphxGraph3.edges.count()}")
    val graphxTime31 = System.nanoTime()
    graphxGraph3.unpersist()
    grapeGraph3.unpersist()

    //1. map edge iterator
    val grapeTime40 = System.nanoTime()
    val grapeGraph4 = mapEdgeIterator(mapEdgeIterator(mapEdgeIterator(grapeGraph)))
    log.info(s"Finish mapping grape edge iterator, counts vertices ${grapeGraph4.vertices.count()}, edges ${grapeGraph4.edges.count()}")
    val grapeTime41 = System.nanoTime()
    val graphxTime40 = System.nanoTime()
    val graphxGraph4 = mapEdgeIterator(mapEdgeIterator(mapEdgeIterator(graphxGraph)))
    log.info(s"Finish mapping graphx edge iterator, counts vertices ${graphxGraph4.vertices.count()} edges ${graphxGraph4.edges.count()}")
    val graphxTime41 = System.nanoTime()
    graphxGraph4.unpersist()
    grapeGraph4.unpersist()

    //1. map edge triplet
    val grapeTime50 = System.nanoTime()
    val grapeGraph5 = mapTriplet(mapTriplet(mapTriplet(grapeGraph)))
    log.info(s"Finish mapping triplet, counts vertices ${grapeGraph5.vertices.count()}, edges ${grapeGraph5.edges.count()}")
    val grapeTime51 = System.nanoTime()
    val graphxTime50 = System.nanoTime()
    val graphxGraph5 = mapTriplet(mapTriplet(mapTriplet(graphxGraph)))
    log.info(s"Finish mapping triplet, counts vertices ${graphxGraph5.vertices.count()} edges ${graphxGraph5.edges.count()}")
    val graphxTime51 = System.nanoTime()
    graphxGraph5.unpersist()
    grapeGraph5.unpersist()

    //1. map edge triplet iterator
    val grapeTime60 = System.nanoTime()
    val grapeGraph6 = mapTripletIterator(mapTripletIterator(mapTripletIterator(grapeGraph)))
    log.info(s"Finish mapping triplet, counts vertices ${grapeGraph6.vertices.count()}, edges ${grapeGraph6.edges.count()}")
    val grapeTime61 = System.nanoTime()
    val graphxTime60 = System.nanoTime()
    val graphxGraph6 = mapTripletIterator(mapTripletIterator(mapTripletIterator(graphxGraph)))
    log.info(s"Finish mapping triplet, counts vertices ${graphxGraph6.vertices.count()} edges ${graphxGraph6.edges.count()}")
    val graphxTime61 = System.nanoTime()
    graphxGraph6.unpersist()
    grapeGraph6.unpersist()

    log.info(s"[OperatorBench]: map vertices grape time ${(grapeTime11 - grapeTime10) / 1000000} ms, graphx time ${(graphxTime11 - graphxTime10)/ 1000000} ms")
    log.info(s"[OperatorBench]: outer join grape time ${(grapeTime31 - grapeTime30) / 1000000} ms, graphx time ${(graphxTime31 - graphxTime30)/ 1000000} ms")
    log.info(s"[OperatorBench]: map [edges grape] time ${(grapeTime21 - grapeTime20) / 1000000} ms, graphx time ${(graphxTime21 - graphxTime20)/ 1000000} ms")
    log.info(s"[OperatorBench]: map [edge iterator] grape time ${(grapeTime41 - grapeTime40) / 1000000} ms, graphx time ${(graphxTime41 - graphxTime40)/ 1000000} ms")
    log.info(s"[OperatorBench]: map [edge triplet] grape time ${(grapeTime51 - grapeTime50) / 1000000} ms, graphx time ${(graphxTime51 - graphxTime50)/ 1000000} ms")
    log.info(s"[OperatorBench]: map [edge triplet iterator] grape time ${(grapeTime61 - grapeTime60) / 1000000} ms, graphx time ${(graphxTime61 - graphxTime60)/ 1000000} ms")

    sc.stop()
  }
}

