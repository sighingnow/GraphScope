package com.alibaba.graphscope.graphx.test

import com.alibaba.graphscope.graphx.GraphScopeHelper
import com.alibaba.graphscope.graphx.shuffle.EdgeShuffle
import org.apache.spark.graphx.{EdgeTriplet, Graph, GraphLoader, PartitionID, TripletFields, VertexRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object OperatorBench extends Logging{
  def main(array: Array[String]) : Unit = {
    require(array.length == 3)
    val fileName = array(0)
    val partNum = array(1).toInt
    val run = array(2)
    require(run.equals("gs") || run.equals("graphx"))
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc.getConf.registerKryoClasses(Array(classOf[EdgeShuffle[_,_]], classOf[Array[Long]], classOf[Array[Int]]))

//    val maskGraph : Graph[Long,Long] = graphxGraph.subgraph(epred = (_ => true), vpred = (id, vd) => id % 2 == 0)
//    val grapeMaskGraph : Graph[Long,Long] = grapeGraph.subgraph(epred = (_ => true), vpred = (id, _)=>id % 2 == 0)

    def mapping(graph : Graph[Long,Long])  : Graph[Long,Long] = {
      graph.mapVertices((vid, vd) => vd + vid)
//        .mapEdges(edge=> edge.srcId + edge.dstId + edge.attr)
//        .mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr + triplet.attr + triplet.srcId + triplet.dstId)
    }

    def mapDifferentType(graph : Graph[Long,Long]) : Graph[Long,Long] = {
      val tmp = graph.mapVertices((vid, vd) => vd.toDouble) //.mapEdges(edge=> edge.attr.toDouble)
      tmp.mapVertices((vid, vd) => vd.toLong) //.mapEdges(edge=> edge.attr.toLong)
    }

    def outerJoin(graph : Graph[Long,Long], degree : VertexRDD[Int]) : Graph[Long,Long] = {
      graph.joinVertices(degree)((id, ovd, newVd) => {
        newVd
      })
    }

    def subgraph(graph: Graph[Long,Long]) : Graph[Long,Long] = {
      graph.subgraph(epred = { triplet => triplet.srcId > 100}, vpred = (vid,vd) => vid > 100)
        .subgraph(epred = {triplet => triplet.srcId > 1000}, vpred = (vid,vd) => vid > 1000)
        .subgraph(epred = triplet => triplet.srcId > 10000, vpred = (vid,vd) => vid > 10000)
    }

    def reverse(graph : Graph[Long,Long]) : Graph[Long,Long] = {
      graph.reverse.reverse.reverse
    }

    //map edge attr to pid.
    def mapEdgeIterator(graph : Graph[Long,Long]) : Graph[Long,Long] = {
      graph.mapEdges((pid,iter)=> iter.map(e=>e.srcId))
    }

    def mapEdges(graph : Graph[Long,Long]) : Long = {
      val time0 = System.nanoTime()
      val tmp0 = graph.mapEdges(edge => {
        edge.dstId + edge.srcId
      })
      val tmp1 = tmp0.mapEdges(edge => {
        edge.dstId + edge.srcId
      })
      val tmp2 = tmp1.mapEdges(edge => {
        edge.dstId + edge.srcId
      })
      val tmp3 = tmp2.mapEdges(edge => {
        edge.dstId + edge.srcId
      })
      val tmp4 = tmp3.mapEdges(edge => {
        edge.dstId + edge.srcId
      })
      val tmp5 = tmp4.mapEdges(edge => {
        edge.dstId + edge.srcId
      })
      val time1 = System.nanoTime()
      log.info(s"[Operator Bench------]Finish mapping edge, counts vertices ${tmp5.vertices.count()}, edges ${tmp5.edges.count()}")
      tmp5.unpersist()
      tmp4.unpersist()
      tmp3.unpersist()
      tmp2.unpersist()
      tmp1.unpersist()
      tmp0.unpersist()
      time1 - time0
    }

    def mapTriplet(graph : Graph[Long,Long]) :Long = {
      val time0 = System.nanoTime()
      val tmp0 = graph.mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr)
      val tmp1 = tmp0.mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr)
      val tmp2 = tmp1.mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr)
      val tmp3 = tmp2.mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr)
      val tmp4 = tmp3.mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr)
      val tmp5 = tmp4.mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr)
      val time1 = System.nanoTime()
      log.info(s"[Operator Bench------]Finish mapping triplet, counts vertices ${tmp5.vertices.count()}, edges ${tmp5.edges.count()}")
      tmp5.unpersist()
      tmp4.unpersist()
      tmp3.unpersist()
      tmp2.unpersist()
      tmp1.unpersist()
      tmp0.unpersist()
      time1 - time0
    }

    def mapTripletIterator(graph : Graph[Long,Long]) : Graph[Long,Long] = {
      def f(pid : PartitionID, iter : Iterator[EdgeTriplet[Long,Long]]): Iterator[Long] = {
        iter.map(triplet=> triplet.srcId)
      }
      graph.mapTriplets[Long]((pid,iter)=>f(pid,iter), TripletFields.All)
    }

    def subGraph(graph : Graph[Long,Long]) : Graph[Long,Long] = {
      graph.subgraph(epred = triplet => triplet.attr % 2 == 1, vpred = (vid,vd) => vd >=0)
    }

    //0. mapping vertices
    if (run.equals("gs")){
      val grapeTime00 =System.nanoTime()
      val grapeGraph = GraphScopeHelper.edgeListFile(sc, fileName,false,partNum)
        .mapVertices((vid,vd) => vd.toLong).mapEdges(edge=>edge.attr.toLong).persist(StorageLevel.MEMORY_ONLY)
      log.info(s"grape graph ${grapeGraph.numVertices},edges ${grapeGraph.numEdges}")
      val grapeTime01 = System.nanoTime()

      val mapEdgesTime = mapEdges(grapeGraph)
      log.info(s"[OperatorBench]: map [edges grape] time ${mapEdgesTime / 1000000} ms")

      val mapTripletTime = mapTriplet(grapeGraph)
      log.info(s"[OperatorBench]: map [edge triplet] grape time ${mapTripletTime / 1000000} ms")

      log.info(s"[Operator Bench:] load graph cost ${(grapeTime01 - grapeTime00)/1000000} ms")
      log.info(s"[OperatorBench]: map [edges grape] time ${mapEdgesTime / 1000000} ms")
      log.info(s"[OperatorBench]: map [edge triplet] grape time ${mapTripletTime / 1000000} ms")

      return

      /*
      val grapeTime10 = System.nanoTime()
      val grapeGraph1 = mapDifferentType(mapDifferentType(mapDifferentType(mapping(mapping(mapping(grapeGraph))))))
      log.info(s"[Operator Bench------] Finish mapping grape vertices ${grapeGraph1.vertices.count()}")
      val grapeTime11 = System.nanoTime()
      log.info(s"[OperatorBench]: map vertices grape time ${(grapeTime11 - grapeTime10) / 1000000} ms")
      grapeGraph1.unpersist()

      val grapeTime20 = System.nanoTime()
      val grapeGraph2 = mapEdges(mapEdges(mapEdges(grapeGraph)))
      log.info(s"[Operator Bench------]Finish mapping grape edge, counts vertices ${grapeGraph2.vertices.count()}, edges ${grapeGraph2.edges.count()}")
      val grapeTime21 = System.nanoTime()
      log.info(s"[OperatorBench]: map [edges grape] time ${(grapeTime21 - grapeTime20) / 1000000} ms")
      grapeGraph2.unpersist()

      val grapeTime300 = System.nanoTime()
      val inDegrees = grapeGraph.inDegrees.cache()
      log.info(s"[Operator Bench------]Finish get degree vertices ${inDegrees.count()}}")
      val grapeTime301 = System.nanoTime()
      log.info(s"[OperatorBench]: get degree time ${(grapeTime301 - grapeTime300) / 1000000} ms")

      val grapeTime30 = System.nanoTime()
      val grapeGraph3 = outerJoin(outerJoin(outerJoin(grapeGraph,inDegrees), inDegrees), inDegrees)
      log.info(s"[Operator Bench------]Finish join, counts vertices ${grapeGraph3.vertices.count()}, edges ${grapeGraph3.edges.count()}")
      val grapeTime31 = System.nanoTime()
      log.info(s"[OperatorBench]: outer join grape time ${(grapeTime31 - grapeTime30) / 1000000} ms")
      grapeGraph3.unpersist()
      inDegrees.unpersist()

//      //1. map edge iterator
//      val grapeTime40 = System.nanoTime()
//      val grapeGraph4 = mapEdgeIterator(mapEdgeIterator(mapEdgeIterator(grapeGraph)))
//      log.info(s"[Operator Bench------]Finish mapping grape edge iterator, counts vertices ${grapeGraph4.vertices.count()}, edges ${grapeGraph4.edges.count()}")
//      val grapeTime41 = System.nanoTime()
//      log.info(s"[OperatorBench]: map [edge iterator] grape time ${(grapeTime41 - grapeTime40) / 1000000} ms")
//      grapeGraph4.unpersist()

      val grapeTime50 = System.nanoTime()
      val grapeGraph5 = mapTriplet(mapTriplet(mapTriplet(grapeGraph)))
      log.info(s"[Operator Bench------]Finish mapping triplet, counts vertices ${grapeGraph5.vertices.count()}, edges ${grapeGraph5.edges.count()}")
      val grapeTime51 = System.nanoTime()
      log.info(s"[OperatorBench]: map [edge triplet] grape time ${(grapeTime51 - grapeTime50) / 1000000} ms")
      grapeGraph5.unpersist()

//      //1. map edge triplet iterator
//      val grapeTime60 = System.nanoTime()
//      val grapeGraph6 = mapTripletIterator(mapTripletIterator(mapTripletIterator(grapeGraph)))
//      log.info(s"[Operator Bench------]Finish mapping triplet iterator, counts vertices ${grapeGraph6.vertices.count()}, edges ${grapeGraph6.edges.count()}")
//      val grapeTime61 = System.nanoTime()
//      log.info(s"[OperatorBench]: map [edge triplet iterator] grape time ${(grapeTime61 - grapeTime60) / 1000000} ms")
//      grapeGraph6.unpersist()

      val grapeTime70 = System.nanoTime()
      val grapeGraph7 = subGraph(grapeGraph)
      log.info(s"[Operator Bench] Finish subgraph, counts vertices ${grapeGraph7.vertices.count()} edges ${grapeGraph7.edges.count()}")
      val grapeTime71 = System.nanoTime()

      val grapeTime80 = System.nanoTime()
      val grapeGraph8 = reverse(grapeGraph)
      log.info(s"[Operator Bench] Finish reverse, counts vertices ${grapeGraph8.vertices.count()} edges ${grapeGraph8.edges.count()}")
      val grapeTime81 = System.nanoTime()

      log.info(s"[OperatorBench]: map vertices grape time ${(grapeTime11 - grapeTime10) / 1000000} ms")
      log.info(s"[OperatorBench]: outer join grape time ${(grapeTime31 - grapeTime30) / 1000000} ms")
      log.info(s"[OperatorBench]: get degree time ${(grapeTime301 - grapeTime300) / 1000000} ms")
      log.info(s"[OperatorBench]: map [edges grape] time ${(grapeTime21 - grapeTime20) / 1000000} ms")
//      log.info(s"[OperatorBench]: map [edge iterator] grape time ${(grapeTime41 - grapeTime40) / 1000000} ms")
      log.info(s"[OperatorBench]: map [edge triplet] grape time ${(grapeTime51 - grapeTime50) / 1000000} ms")
//      log.info(s"[OperatorBench]: map [edge triplet iterator] grape time ${(grapeTime61 - grapeTime60) / 1000000} ms")
      log.info(s"[OperatorBench] load graph cost ${(grapeTime01 - grapeTime00)/1000000}ms")
      log.info(s"[OperatorBench]: sub graph grape time ${(grapeTime71 - grapeTime70) / 1000000} ms")
      log.info(s"[OperatorBench]: reverse graph grape time ${(grapeTime81 - grapeTime80) / 1000000} ms")
      */
    }
    else if (run.equals("graphx")){
      val graphxTime00 = System.nanoTime()
      val rawGraph = GraphLoader.edgeListFile(sc, fileName,false, partNum)
      val graphxGraph = rawGraph.mapVertices((vid,vd)=>vd.toLong).mapEdges(edge=>edge.attr.toLong).persist(StorageLevel.MEMORY_ONLY)
      log.info(s"graphx graph ${graphxGraph.vertices.count()}, ${graphxGraph.edges.count()}")
      val graphxTime01 = System.nanoTime()

      val mapEdgesTime = mapEdges(graphxGraph)
      log.info(s"[Operator Bench------]Finish mapping edges graphx cost ${mapEdgesTime/ 1000000}ms")

      val mapTripletTime = mapTriplet(graphxGraph)
      log.info(s"[Operator Bench------]Finish mapping triplet graphx, cost time ${mapTripletTime}")

      log.info(s"[Operator Bench:] load graph cost ${(graphxTime01 - graphxTime00)/1000000} ms")
      log.info(s"[OperatorBench]: map [edges ] graphx time ${mapEdgesTime / 1000000} ms")
      log.info(s"[OperatorBench]: map [edge triplet] graphx time ${mapTripletTime/ 1000000} ms")
      log.info(s"[OperatorBench]: partition num ${graphxGraph.vertices.getNumPartitions}")

      return

      /*
      val graphxTime10 = System.nanoTime()
      val graphxGraph1 = mapDifferentType(mapDifferentType(mapDifferentType(mapping(mapping(mapping(graphxGraph))))))
      log.info(s"[Operator Bench------]Finish mapping graphx vertices ${graphxGraph1.vertices.count()}")
      val graphxTime11 = System.nanoTime()
      graphxGraph1.unpersist()

      val graphxTime20 = System.nanoTime()
      val graphxGraph2 = mapEdges(mapEdges(mapEdges(graphxGraph)))
      log.info(s"[Operator Bench------]Finish mapping graphx edge, counts vertices ${graphxGraph2.vertices.count()} edges ${graphxGraph2.edges.count()}")
      val graphxTime21 = System.nanoTime()
      graphxGraph2.unpersist()

      val graphxTime300 = System.nanoTime()
      val inDegrees = graphxGraph.inDegrees.cache()
      log.info(s"[Operator Bench------]Finish get degree vertices ${inDegrees.count()}}")
      val graphxTime301 = System.nanoTime()
      log.info(s"[OperatorBench]: get degree time ${(graphxTime301 - graphxTime300) / 1000000} ms")

      val graphxTime30 = System.nanoTime()
      val graphxGraph3 = outerJoin(outerJoin(outerJoin(graphxGraph, inDegrees),inDegrees),inDegrees)
      log.info(s"[Operator Bench------]Finish join, counts vertices ${graphxGraph3.vertices.count()} edges ${graphxGraph3.edges.count()}")
      val graphxTime31 = System.nanoTime()
      graphxGraph3.unpersist()
      inDegrees.unpersist()

//      val graphxTime40 = System.nanoTime()
//      val graphxGraph4 = mapEdgeIterator(mapEdgeIterator(mapEdgeIterator(graphxGraph)))
//      log.info(s"[Operator Bench------]Finish mapping graphx edge iterator, counts vertices ${graphxGraph4.vertices.count()} edges ${graphxGraph4.edges.count()}")
//      val graphxTime41 = System.nanoTime()
//      graphxGraph4.unpersist()

      val graphxTime50 = System.nanoTime()
      val graphxGraph5 = mapTriplet(mapTriplet(mapTriplet(graphxGraph)))
      log.info(s"[Operator Bench------]Finish mapping triplet, counts vertices ${graphxGraph5.vertices.count()} edges ${graphxGraph5.edges.count()}")
      val graphxTime51 = System.nanoTime()
      graphxGraph5.unpersist()

//      val graphxTime60 = System.nanoTime()
//      val graphxGraph6 = mapTripletIterator(mapTripletIterator(mapTripletIterator(graphxGraph)))
//      log.info(s"[Operator Bench------]Finish mapping triplet iterator, counts vertices ${graphxGraph6.vertices.count()} edges ${graphxGraph6.edges.count()}")
//      val graphxTime61 = System.nanoTime()
//      graphxGraph6.unpersist()

      val graphxTime70 = System.nanoTime()
      val graphxGraph7 = subGraph(graphxGraph)
      log.info(s"[Operator Bench] Finish sub graph, counts vertices ${graphxGraph7.vertices.count()} edges ${graphxGraph7.edges.count()}")
      val graphxTime71 = System.nanoTime()

      val graphxTime80 = System.nanoTime()
      val graphxGraph8 = reverse(graphxGraph)
      log.info(s"[Operator Bench] Finish reverse, counts vertices ${graphxGraph8.vertices.count()} edges ${graphxGraph8.edges.count()}")
      val graphxTime81 = System.nanoTime()

      log.info(s"[OperatorBench]: map vertices graphx time ${(graphxTime11 - graphxTime10)/ 1000000} ms")
      log.info(s"[OperatorBench]: outer join graphx time ${(graphxTime31 - graphxTime30)/ 1000000} ms")
      log.info(s"[OperatorBench]: get degree time ${(graphxTime301 - graphxTime300) / 1000000} ms")
      log.info(s"[OperatorBench]: map [edges ] graphx time ${(graphxTime21 - graphxTime20)/ 1000000} ms")
      log.info(s"[OperatorBench]: map [edge iterator] graphx time ${(graphxTime41 - graphxTime40)/ 1000000} ms")
      log.info(s"[OperatorBench]: map [edge triplet] graphx time ${(graphxTime51 - graphxTime50)/ 1000000} ms")
      log.info(s"[OperatorBench]: map [edge triplet iterator] graphx time ${(graphxTime61 - graphxTime60)/ 1000000} ms")
      log.info(s"[OperatorBench] load graph cost ${(graphxTime01 - graphxTime00)/1000000}ms")
      log.info(s"[OperatorBench]: subGraph cost ${(graphxTime71 - graphxTime70)/1000000} ms")
      log.info(s"[OperatorBench]: reverse graph cost ${(graphxTime81 - graphxTime80)/1000000} ms")

       */
    }
    else {
      throw new IllegalStateException(s"not recognized ${run}")
    }

    sc.stop()
  }
}

