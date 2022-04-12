package com.alibaba.graphscope.example

import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.collection.PrimitiveVector

import scala.collection.mutable.ArrayBuffer

object SSSP {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 4){
      println("Expect 2 args")
      return 0;
    }
    val vfilePath= args(0);
    val efilePath = args(1)
    val numParition = args(2)
    val sourceId: VertexId = args(3).toLong // The ultimate source
    println("vfile" + vfilePath  + "efile " + efilePath + "source : " + sourceId)

    val lines = sc.textFile(efilePath, numParition.toInt)
    val edgesRDD = lines.mapPartitionsWithIndex( (pid, iter) =>{
      var edges = new ArrayBuffer[Edge[Double]]
      iter.foreach{
        line => {
          if (!line.isEmpty && line(0) != '#') {
            val lineArray = line.split("\\s+")
            if (lineArray.length < 3) {
              throw new IllegalArgumentException("Invalid line: " + line)
            }
            val srcId = lineArray(0).toLong
            val dstId = lineArray(1).toLong
            val edata = lineArray(2).toDouble
            edges.+=(new Edge[Double](srcId,dstId,edata))
          }
        }
      }
      edges.iterator
    })
    val verticesLines = sc.textFile(vfilePath, numParition.toInt)
    val verticesRDD = verticesLines.mapPartitionsWithIndex(
      (pid, iter) =>{
        var vertices = new ArrayBuffer[(VertexId, Double)]
        iter.foreach{
          line => {
            if (!line.isEmpty && line(0) != '#') {
              val lineArray = line.split("\\s+")
              if (lineArray.length < 3) {
                throw new IllegalArgumentException("Invalid line: " + line)
              }
              val vid = lineArray(0).toLong
              val vdata = lineArray(1).toDouble
              vertices.+=((vid,vdata))
            }
          }
        }
        vertices.iterator
      }
    )
    ///home/graphscope/data/gstest/p2p-31.e
    println("edge rdd num partitions: " + edgesRDD.getNumPartitions)
    println("edge rdd partitioner: " + edgesRDD.partitioner)
    println("vertex rdd num partitions: " + verticesRDD.getNumPartitions)
    println("vertex rdd partitioner: " + verticesRDD.partitioner)
    val newEdgesRDD = edgesRDD.repartition(numParition.toInt)
    val newVerticesRDD = verticesRDD.repartition(numParition.toInt)
    val graph = Graph.apply(newVerticesRDD,newEdgesRDD)
    val verticesNum = graph.vertices.count()
    val edgesSNum = graph.edges.count()
    println(s"Graph has ${verticesNum} vertices, ${edgesSNum} edges")
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, vdata) =>
      if (id == sourceId) 0.0 else vdata.toDouble)
//    println(initialGraph.vertices.collect().mkString("Array(", ", ", ")"))
//    println(initialGraph.edges.collect().mkString("Array(", ", ", ")"))
    val startTime = System.nanoTime();
    val sssp = initialGraph.pregel(Double.PositiveInfinity)( //avoid overflow
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
//          println(triplet.srcAttr + ", " + (triplet.srcAttr + triplet.attr) + ", " + triplet.dstAttr)
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    val endTIme = System.nanoTime()
    println("[Pregel running time ] : " + ((endTIme - startTime) / 1000000) + "ms")
    sssp.vertices.saveAsTextFile("/tmp/spark-graphx")
//    println(sssp.vertices.collect.mkString("\n"))
    // $example off$

    sc.stop()
  }
}
