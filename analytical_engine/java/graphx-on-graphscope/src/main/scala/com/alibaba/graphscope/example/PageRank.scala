package com.alibaba.graphscope.example

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object PageRank {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 6) {
      println("Expect 5 args")
      return 0;
    }
    val vfilePath = args(0);
    val efilePath = args(1)
    val numParition = args(2)
    val numIteration = args(3).toInt
    val tolerance = args(4).toDouble
    val resetProb = args(5).toDouble
    require(tolerance >= 0, s"Tolerance must be no less than 0, but got ${tolerance}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")

    val lines = sc.textFile(efilePath, numParition.toInt)
    val edgesRDD = lines.mapPartitionsWithIndex((pid, iter) => {
      var edges = new ArrayBuffer[Edge[Double]]
      iter.foreach {
        line => {
          if (!line.isEmpty && line(0) != '#') {
            val lineArray = line.split("\\s+")
            if (lineArray.length < 3) {
              throw new IllegalArgumentException("Invalid line: " + line)
            }
            val srcId = lineArray(0).toLong
            val dstId = lineArray(1).toLong
            val edata = lineArray(2).toDouble
            edges.+=(new Edge[Double](srcId, dstId, edata))
          }
        }
      }
      edges.iterator
    })
    val verticesLines = sc.textFile(vfilePath, numParition.toInt)
    val verticesRDD = verticesLines.mapPartitionsWithIndex(
      (pid, iter) => {
        var vertices = new ArrayBuffer[(VertexId, Double)]
        iter.foreach {
          line => {
            if (!line.isEmpty && line(0) != '#') {
              val lineArray = line.split("\\s+")
              if (lineArray.length < 2) {
                throw new IllegalArgumentException("Invalid line: " + line)
              }
              val vid = lineArray(0).toLong
              val vdata = Double.PositiveInfinity
              vertices.+=((vid, vdata))
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
    val graph = Graph.apply(verticesRDD, edgesRDD)

//    val pagerankGraph: Graph[(Double, Double), Double] = graph
//      // Associate the degree with each vertex
//      .outerJoinVertices(graph.outDegrees) {
//        (vid, vdata, deg) => deg.getOrElse(0)
//      }
//      // Set the weight on the edges based on the degree
//      .mapTriplets( e => 1.0 / e.srcAttr )
//      // Set the vertex attributes to (initialPR, delta = 0)
//      .mapVertices { (id, attr) =>
//        (0.0, 0.0)
//      }
//      .cache()
    val pageRankGraph = graph.outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }

    val edgesNum = graph.numEdges
    val verticesNum = graph.numVertices
    println(s"Graph has ${verticesNum} vertices, ${edgesNum} edges")

    def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double = {
      resetProb * verticesNum + (1 - resetProb) * msgSum
    }
//    def sendMsg(edgeTriplet: EdgeTriplet[Double,Double]): Iterator[(VertexId, Double)] = {
//      Iterator((edgeTriplet.dstId, graph.outDegrees.))
//    }
    val startTime = System.nanoTime();

    val endTIme = System.nanoTime()
    println("[Pregel running time ] : " + ((endTIme - startTime) / 1000000) + "ms")
    //    sssp.vertices.saveAsTextFile("/tmp/spark-graphx")
    //    println(sssp.vertices.collect.mkString("\n"))
    // $example off$

    sc.stop()
  }
}
