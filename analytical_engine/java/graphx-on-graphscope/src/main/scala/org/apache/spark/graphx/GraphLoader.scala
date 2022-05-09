package org.apache.spark.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GrapeGraphImpl, GraphImpl}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

import java.util.concurrent.TimeUnit
import scala.reflect.ClassTag

object GraphLoader extends Logging {
  def edgeListFile
  (sc: SparkContext,
   path: String,
   canonicalOrientation: Boolean = false,
   numEdgePartitions: Int = -1,
   edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
   vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : Graph[Long, Long] = {
    val startTimeNs = System.nanoTime()
    // Parse the edge data table directly into edge partitions
    val lines =
      if (numEdgePartitions > 0) {
        sc.textFile(path, numEdgePartitions).coalesce(numEdgePartitions)
      } else {
        sc.textFile(path)
      }
    val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[Int, Int]
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            throw new IllegalArgumentException("Invalid line: " + line)
          }
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          if (canonicalOrientation && srcId > dstId) {
            builder.add(dstId, srcId, 1)
          } else {
            builder.add(srcId, dstId, 1)
          }
        }
      }
      Iterator((pid, builder.toEdgePartition))
    }.persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))
    edges.count()

    logInfo(s"It took ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms" +
      " to load the edges")

    val edgeRDD = GrapeEdgeRDD.fromEdgePartitions(edges)
    //val vertexRDD = GrapeVertexRDD.fromEdgeRDD(edgeRDD, edgeRDD.grapePartitionsRDD.getNumPartitions, 1).cache()
    //log.info(s"num vertices ${vertexRDD.count()}, num edges ${edgeRDD.count()}")
    ////GrapeGraphImpl.fromExistingRDDs(vertexRDD,edgeRDD);
    null
//    GrapeGraphImpl.fromGraphXGraph(res)
  }

//  def edgeListFile(
//                    sc: SparkContext,
//                    path: String,
//                    canonicalOrientation: Boolean = false,
//                    numEdgePartitions: Int = -1,
//                    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
//                    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
//  : Graph[Int, Int] = {
//    val startTimeNs = System.nanoTime()
//    // Parse the edge data table directly into edge partitions
//    val lines =
//      if (numEdgePartitions > 0) {
//        sc.textFile(path, numEdgePartitions).coalesce(numEdgePartitions)
//      } else {
//        sc.textFile(path)
//      }
//    val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
//      val builder = new EdgePartitionBuilder[Int, Int]
//      iter.foreach { line =>
//        if (!line.isEmpty && line(0) != '#') {
//          val lineArray = line.split("\\s+")
//          if (lineArray.length < 2) {
//            throw new IllegalArgumentException("Invalid line: " + line)
//          }
//          val srcId = lineArray(0).toLong
//          val dstId = lineArray(1).toLong
//          if (canonicalOrientation && srcId > dstId) {
//            builder.add(dstId, srcId, 1)
//          } else {
//            builder.add(srcId, dstId, 1)
//          }
//        }
//      }
//      Iterator((pid, builder.toEdgePartition))
//    }.persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))
//    edges.count()
//
//    logInfo(s"It took ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms" +
//      " to load the edges")
//
//    GraphImpl.fromEdgePartitions(edges, defaultVertexAttr = 1, edgeStorageLevel = edgeStorageLevel,
//      vertexStorageLevel = vertexStorageLevel)
//  } // end of edgeListFile
}
