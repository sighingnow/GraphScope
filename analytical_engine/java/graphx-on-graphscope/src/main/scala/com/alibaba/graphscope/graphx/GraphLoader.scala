package com.alibaba.graphscope.graphx

import org.apache.spark.graphx.impl.GrapeEdgePartitionBuilder
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.reflect.ClassTag

object GraphLoader extends Logging {
  def edgeListFile[VD: ClassTag, ED: ClassTag]
  (sc: SparkContext,
   path: String,
   defaultEdata: ED,
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

    val parsedLines: RDD[(Long, Edge[ED])] = lines.map(line => {
        val lineArray = line.split("\\s+")
        if (lineArray.length < 2) {
          throw new IllegalArgumentException("Invalid line: " + line)
        }
        val srcId = lineArray(0).toLong //partition on srcId
        val dstId = lineArray(1).toLong
        (srcId, new Edge[ED](srcId, dstId, defaultEdata))
    }).persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))

    val shuffledEdges = parsedLines.partitionBy(new HashPartitioner(numEdgePartitions))
    log.info(s"${shuffledEdges.collect().mkString("Array(", ", ", ")")}")
    val totalEdges = shuffledEdges.count()
    val t1 = System.nanoTime();
    val loadEdgeTime = (t1 - startTimeNs) / 1000000

    val shuffledEdgePartitions = shuffledEdges.mapPartitionsWithIndex(
      {
        (pid, iter) =>{
          //For a iterator of (src,Edge), convert it to a edgePartition which store them in columar format
          val builder = new GrapeEdgePartitionBuilder[ED](pid, numEdgePartitions, shuffledEdges.partitioner.get)
          while (iter.hasNext){
            val item = iter.next()
            builder.add(item._2)
          }
          Iterator(pid, builder.toGrapeEdgePartition)
        }
      }, true
    )

//    val grapeEdgeRDD = GrapeEdgeRDD.fromRDD(shuffledEdges)
    log.info(s"Load total edges ${totalEdges}, num partitions: ${shuffledEdges.getNumPartitions}, cost ${loadEdgeTime}ms")


    null
  }
}
