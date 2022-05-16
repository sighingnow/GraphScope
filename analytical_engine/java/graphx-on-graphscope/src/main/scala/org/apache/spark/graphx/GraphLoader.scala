package org.apache.spark.graphx

import org.apache.spark.graphx.impl.GrapeGraphImpl
import org.apache.spark.graphx.impl.partition.EdgeShuffle
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{OpenHashSet, PrimitiveVector}
import org.apache.spark.{HashPartitioner, SparkContext}

import java.util.concurrent.TimeUnit

object GraphLoader extends Logging {
  def edgeListFile
  (sc: SparkContext,
   path: String,
   canonicalOrientation: Boolean = false,
   numEdgePartitions: Int = -1,
   edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
   vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : Graph[Int, Int] = {
    val startTimeNs = System.nanoTime()
    // Parse the edge data table directly into edge partitions
    val lines = {
      if (numEdgePartitions > 0) {
        sc.textFile(path, numEdgePartitions).coalesce(numEdgePartitions)
      } else {
        sc.textFile(path)
      }
    }
    val numLines = lines.count() / numEdgePartitions
    val partitioner = new HashPartitioner(numEdgePartitions)
    val edgesShuffled = lines.mapPartitionsWithIndex {
      (fromPid, iter) => {
        val pid2src = Array.fill(numEdgePartitions)(new PrimitiveVector[VertexId](numLines.toInt))
        val pid2Dst = Array.fill(numEdgePartitions)(new PrimitiveVector[VertexId](numLines.toInt))
        val pid2attr = Array.fill(numEdgePartitions)(new PrimitiveVector[Int](numLines.toInt))
        val pid2Oids = Array.fill(numEdgePartitions)(new OpenHashSet[VertexId](numLines.toInt / 2))
        while (iter.hasNext) {
          val lineArray = iter.next().split("\\s+")
          if (lineArray.length < 2) {
            throw new IllegalArgumentException("Invalid line: ")
          }
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          val srcPid = partitioner.getPartition(srcId)
          val dstPid = partitioner.getPartition(dstId)
          pid2Oids(srcPid).add(srcId)
          pid2Oids(srcPid).add(dstId)
          if (srcPid == dstPid){
            pid2src(srcPid).+=(srcId)
            pid2Dst(srcPid).+=(dstId)
            pid2attr(srcPid)+=(1)
          }
          else {
            pid2src(srcPid).+=(srcId)
            pid2Dst(srcPid).+=(dstId)
            pid2attr(srcPid).+=(1)
            pid2src(dstPid).+=(srcId)
            pid2Dst(dstPid).+=(dstId)
            pid2attr(dstPid).+=(1)
          }
        }
        pid2src.zipWithIndex.map({
          case (srcs, pid) => (pid, new EdgeShuffle(fromPid,pid, pid2Oids(pid), srcs.trim().array, pid2Dst(pid).trim().array, pid2attr(pid).trim().array))
        }).toIterator
      }
    }.partitionBy(partitioner).persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))
    val edgeShufflesNum = edgesShuffled.count()
    log.info(s"total edge shuffles invoked ${edgeShufflesNum}")

    logInfo(s"It took ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms" +
      " to load the edges")

    val time0 = System.nanoTime()
    val edgeRDD = GrapeEdgeRDD.fromEdgeShuffle[Int,Int](edgesShuffled).cache()
    val time1 = System.nanoTime()
    log.info(s"[GraphLoader:] construct edge rdd ${edgeRDD} cost ${(time1 - time0) / 1000000} ms")
    val vertexRDD = GrapeVertexRDD.fromEdgeRDD[Int](edgeRDD, edgeRDD.grapePartitionsRDD.getNumPartitions, 1,vertexStorageLevel).cache()
    log.info(s"num vertices ${vertexRDD.count()}, num edges ${edgeRDD.count()}")
    GrapeGraphImpl.fromExistingRDDs(vertexRDD,edgeRDD)
  }
}
