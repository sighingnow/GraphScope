package com.alibaba.graphscope.graphx.test

import com.alibaba.graphscope.graphx.GraphScopeHelper
import com.alibaba.graphscope.graphx.shuffle.EdgeShuffle
import org.apache.spark.graphx.grape.GrapeGraphImpl
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object ComprehensiveTest extends Logging{
  def main(array: Array[String]) : Unit = {
    require(array.length == 3)
    val fileName = array(0)
    val partNum = array(1).toInt
    val engine = array(2)
    require(engine.equals("gs") || engine.equals("graphx"))
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc.getConf.registerKryoClasses(Array(classOf[EdgeShuffle[_,_]], classOf[Array[Long]], classOf[Array[Int]]))

    def runGrape(graph : GrapeGraphImpl[Long,Long]) : Long = {
      val time0 = System.nanoTime()
      graph.grapeEdges.grapePartitionsRDD.foreachPartition(iter => {
        if (iter.hasNext){
          val part = iter.next()
          part.emptyIteration
        }
      })
      val time1 = System.nanoTime()
      time1 - time0
//      log.info(s"Iterate over grape cost ${(time1 - time0)/1000000} ms")
    }

    def runGraphx(graph : Graph[Long,Long]) : Long = {
      val time0 = System.nanoTime()
      graph.edges.foreachPartition(iter => {
        while (iter.hasNext){
          val tuple = iter.next()
        }
      })
      val time1 = System.nanoTime()
      time1 - time0
//      log.info(s"Iterate over graphx cost ${(time1 - time0)/1000000} ms")
    }

    if (engine.equals("gs")) {
      val grapeTime00 = System.nanoTime()
      val grapeGraph = GraphScopeHelper.edgeListFile(sc, fileName, false, partNum)
        .mapVertices((vid, vd) => vd.toLong).mapEdges(edge => edge.attr.toLong).persist(StorageLevel.MEMORY_ONLY)
      log.info(s"grape graph ${grapeGraph.numVertices},edges ${grapeGraph.numEdges}")
      val grapeTime01 = System.nanoTime()
      log.info(s"[Comprehensive] load graph cost ${(grapeTime01 - grapeTime00)/1000000} ms")
      val iterPart = runGrape(grapeGraph.asInstanceOf[GrapeGraphImpl[Long,Long]])
      val iteratorTime = runGraphx(grapeGraph)

      log.info(s"[Comprehensive] grape iterate edges partitions cost ${iterPart/ 1000000} ms")
      log.info(s"[Comprehensive] grape iterate edges cost ${iteratorTime/1000000} ms")
    }
    else if (engine.equals("graphx")){
      val graphxTime00 = System.nanoTime()
      val rawGraph = GraphLoader.edgeListFile(sc, fileName,false, partNum)
      val graphxGraph = rawGraph.mapVertices((vid,vd)=>vd.toLong).mapEdges(edge=>edge.attr.toLong).persist(StorageLevel.MEMORY_ONLY)
      log.info(s"graphx graph ${graphxGraph.vertices.count()}, ${graphxGraph.edges.count()}")
      val graphxTime01 = System.nanoTime()

      log.info(s"[Comprehensive] load graph cost ${(graphxTime01 - graphxTime00)/1000000} ms")
      val iteratorTime = runGraphx(graphxGraph)

      log.info(s"[Comprehensive] graphx iterate edges cost ${iteratorTime/1000000} ms")
    }

  }

}
