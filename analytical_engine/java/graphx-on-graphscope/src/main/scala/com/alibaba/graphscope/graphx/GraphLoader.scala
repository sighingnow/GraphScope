package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.graphx.partition.{RawEdgePartitionBuilder}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.graphx.Graph
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object GraphLoader extends Logging{
  def edgeListFile[VD : ClassTag, ED: ClassTag]
      (sc: SparkContext,
          path: String,
       defaultEdata  : ED,
          canonicalOrientation: Boolean = false,
          numEdgePartitions: Int = -1,
          edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
          vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : Graph[VD, ED] = {
    val startTimeNs = System.nanoTime()
    // Parse the edge data table directly into edge partitions
    val lines =
      if (numEdgePartitions > 0) {
        sc.textFile(path, numEdgePartitions).coalesce(numEdgePartitions)
      } else {
        sc.textFile(path)
      }
    val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new RawEdgePartitionBuilder[Long,Long,ED]
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            throw new IllegalArgumentException("Invalid line: " + line)
          }
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          if (canonicalOrientation && srcId > dstId) {
            builder.add(dstId, srcId, defaultEdata)
          } else {
            builder.add(srcId, dstId, defaultEdata)
          }
        }
      }
      Iterator((pid, builder.toRawEdgePartition))
    }.persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))

    val edgePartitions = edges.partitionBy(new HashPartitioner(numEdgePartitions))
    val totalEdges = edgePartitions.count()
    val t1 = System.nanoTime();
    val loadEdgeTime = (t1 - startTimeNs) / 1000000
    log.info(s"Load total edges ${totalEdges}, num partitions: ${edgePartitions.getNumPartitions}, cost ${loadEdgeTime}ms")

    null
  }
}
