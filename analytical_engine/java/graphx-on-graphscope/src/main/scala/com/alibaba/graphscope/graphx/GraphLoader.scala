package com.alibaba.graphscope.graphx

import org.apache.spark.graphx.impl.{GrapeEdgePartitionBuilder, GrapeGraphImpl}
import org.apache.spark.graphx.{Edge, GrapeEdgeRDD, Graph, PartitionID}
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

    val linesCount = lines.count()
    val partitioner = new HashPartitioner(numEdgePartitions)
    val allEdges: RDD[(PartitionID, Edge[ED])] = lines.flatMap(line => {
        val lineArray = line.split("\\s+")
        if (lineArray.length < 2) {
          throw new IllegalArgumentException("Invalid line: " + line)
        }
        val srcId = lineArray(0).toLong //partition on srcId
        val dstId = lineArray(1).toLong
        val srcPid = partitioner.getPartition(srcId)
        val dstPid = partitioner.getPartition(dstId)
      if (srcPid == dstPid) {
        Iterator((srcPid, new Edge[ED](srcId, dstId, defaultEdata)))
      }
      else {
        Iterator((srcPid, new Edge[ED](srcId, dstId, defaultEdata)),(dstPid, new Edge[ED](srcId, dstId, defaultEdata)))
      }
    })

    val shuffledEdges = allEdges.partitionBy(partitioner).cache()
//    log.info(s"${shuffledEdges.collect().mkString("Array(", ", ", ")")}")
    val distributedEdges = shuffledEdges.count()
    log.info(s"Original edges ${linesCount}, after shuffle ${distributedEdges}")

    val t1 = System.nanoTime();
    val loadEdgeTime = (t1 - startTimeNs) / 1000000

    //The edges shuffled to us are in to folders
    //1. srcId belongs to us.
    //2. dstId belongs to us
    val shuffledEdgePartitions = shuffledEdges.mapPartitionsWithIndex(
      {
        (pid, iter) =>{
          //For a iterator of (src,Edge), convert it to a edgePartition which store them in columar format
          val builder = new GrapeEdgePartitionBuilder[ED](pid, numEdgePartitions, shuffledEdges.partitioner.get)
          while (iter.hasNext){
            val item = iter.next()
            builder.add(item._2)
          }
          Iterator((pid, builder.toGrapeEdgePartition))
        }
      }, true
    ).persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))
    log.info("shuffledEdgePartitions: " + shuffledEdgePartitions.count()) //FIXME: printed 4 here

    val edgeRDD = GrapeEdgeRDD.fromEdgePartitions(shuffledEdgePartitions)
    log.info("EdgeRDD count: " + edgeRDD.count())
    log.info(s"Load total edges ${linesCount}, sum of edges in all frag ${distributedEdges} num partitions: ${shuffledEdges.getNumPartitions}, cost ${loadEdgeTime}ms")
    val graph = GrapeGraphImpl.fromEdgeRDD(edgeRDD, null.asInstanceOf[VD])
    log.info(s"total vertex count ${graph.numVertices}, total edges count ${graph.numEdges}")
    log.info("[Now construct graph]")

    graph
  }
}
