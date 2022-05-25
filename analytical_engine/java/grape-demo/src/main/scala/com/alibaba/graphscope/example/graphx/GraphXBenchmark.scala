package com.alibaba.graphscope.example.graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object GraphXBenchmark extends Logging{
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 2) {
      println("Expect 2 args")
      return 0;
    }
    val efilePath = args(0)
    val numPartition = args(1).toInt
    log.info(s"efile path ${efilePath}, numPartition ${numPartition}")
    val loadGraph0 = System.nanoTime();
    val graph = GraphLoader.edgeListFile(sc, efilePath, canonicalOrientation = false, numPartition).cache()
    log.info(s"Graph vertices ${graph.numVertices}, edges ${graph.numEdges}")
    val loadGraph1 = System.nanoTime();
    /**
     * Test graphx vertex transformation
     */
    val graph1 = graph.mapVertices((vid, vd)=> vid + vd)
    val graph2 = graph1.mapVertices((vid, vd)=> vd.toLong)
    val graph3 = graph2.mapVertices((vid, vd)=> vd + vid)
    val graph4 = graph3.mapVertices((vid, vd)=> vd + vid)
    val graph5 = graph4.mapVertices((vid, vd)=> vd + vid)
    val graph6 = graph5.mapVertices((vid, vd)=> vd + vid)
    log.info(s"after map vertices ${graph6.numVertices} ${graph6.numEdges}")
    val time1 = System.nanoTime()
    /**
     * test edge transformation
     */
    val graph7 = graph6.mapEdges(edge => edge.attr + 1)
    val graph8 = graph7.mapEdges(edge => edge.attr.toLong)
    val graph9 = graph8.mapEdges(edge => edge.srcId + edge.attr)
    val graph10 = graph9.mapEdges(edge => edge.dstId + edge.attr)
    val graph11 = graph10.mapEdges(edge => edge.srcId + edge.attr)
    val graph12 = graph11.mapEdges(edge => edge.dstId + edge.attr)
    log.info(s"after transform edges ${graph12.numVertices} ${graph12.numEdges}")
    val time2 = System.nanoTime()

    /**
     * test map triplet
     */
    val graph13 = graph12.mapTriplets(triplet => triplet.srcAttr + triplet.dstAttr + triplet.attr)
    val graph14 = graph13.mapTriplets(triplet => triplet.attr + 1)
    val graph15 = graph14.mapTriplets(triplet => triplet.attr * 2)
    log.info(s"After transform edge triplet ${graph15.numVertices} ${graph15.numEdges}")
    val time3 = System.nanoTime()

    /**
     * test join
     */
    val outDegree = graph15.outDegrees.cache()
    val inDegree = graph15.inDegrees.cache()
    log.info(s"after get out degree and in degrees ${outDegree.count()}, ${inDegree.count()}")
    val time31 = System.nanoTime()
    val graph16 = graph15.outerJoinVertices(outDegree)((_,_,degree) => degree.getOrElse(0))
    val graph17 = graph16.outerJoinVertices(inDegree)((_,_,degree)=>degree.getOrElse(0))
    log.info(s"after outer join ${graph17.vertices.map(tuple => tuple._2).count()}")
    val time4 = System.nanoTime()

//    log.info(s"s[Summary: ] graph6 vertices${graph6.vertices.collect().map(tuple => tuple._2).sum}")
//    log.info(s"s[Summary: ] graph12 edges ${graph12.vertices.collect().map(tuple => tuple._2).sum}")
//    log.info(s"s[Summary: ] graph15 triplets ${graph15.vertices.collect().map(tuple => tuple._2).sum}")
//    log.info(s"s[Summary: ] graph6 vertices ${graph6.triplets.collect().map(tuple => tuple.attr).sum}")
//    log.info(s"s[Summary: ] graph12 edges ${graph12.triplets.collect().map(tuple => tuple.attr).sum}")
//    log.info(s"s[Summary: ] graph15 triplets ${graph15.triplets.collect().map(tuple => tuple.attr).sum}")
    log.info(s"[Summary: ] Load graph cost ${(loadGraph1 - loadGraph0) / 1000000}ms")
    log.info(s"[Summary: ] map vertices cost ${(time1 - loadGraph1) / 1000000}ms")
    log.info(s"[Summary: ] map edges cost ${(time2 - time1) / 1000000}ms")
    log.info(s"[Summary: ] map edge triplets cost ${(time3 - time2) / 1000000}ms")
    log.info(s"[Summary: ] get degree cost ${(time31 - time3) / 1000000}ms")
    log.info(s"[Summary: ] join vertices cost ${(time4 - time31) / 1000000}ms")

  }
}
