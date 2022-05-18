package org.apache.spark.graphx

import com.alibaba.graphscope.utils.{LongLong, LongLongInputFormat}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.graphx.impl.GrapeGraphImpl
import org.apache.spark.graphx.impl.partition.EdgeShuffle
import org.apache.spark.graphx.utils.EdgeShuffleToMe
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{OpenHashSet, PrimitiveVector}
import org.apache.spark.{HashPartitioner, SparkContext}

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

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
//        sc.textFile(path, numEdgePartitions).coalesce(numEdgePartitions)
//        sc.newAPIHadoopFile(path, classOf[LongLongInputFormat], classOf[LongWritable],classOf[LongLong],numEdgePartitions).setName(path).coalesce(numEdgePartitions)
        sc.hadoopFile(path, classOf[LongLongInputFormat], classOf[LongWritable],classOf[LongLong]).coalesce(numEdgePartitions).setName(path)
      } else {
        sc.hadoopFile(path, classOf[LongLongInputFormat], classOf[LongWritable],classOf[LongLong]).setName(path)
      }
    }.map(pair => (pair._2.first, pair._2.second))
    lines.cache()
    val linesTime = System.nanoTime()
    log.info("[GraphLoader]: load partitions cost " + (linesTime - startTimeNs) / 1000000 + "ms")
//    val numLines = lines.count() / numEdgePartitions
    val partitioner = new HashPartitioner(numEdgePartitions)
    val edgesShuffled = lines.mapPartitionsWithIndex (
      (fromPid, iter) => {
//        iter.toArray
        val pid2src = Array.fill(numEdgePartitions)(new PrimitiveVector[VertexId])
        val pid2Dst = Array.fill(numEdgePartitions)(new PrimitiveVector[VertexId])
        val pid2attr = Array.fill(numEdgePartitions)(new PrimitiveVector[Int])
        val pid2Oids = Array.fill(numEdgePartitions)(new OpenHashSet[VertexId])
        val time0 = System.nanoTime();
        while (iter.hasNext) {
//          val lineArray = iter.next().split("\\s+")
//          if (lineArray.length < 2) {
//            throw new IllegalArgumentException("Invalid line: ")
//          }
//          val srcId = lineArray(0).toLong
//          val dstId = lineArray(1).toLong
          val line = iter.next()
          val srcId = line._1
          val dstId = line._2
          val srcPid = partitioner.getPartition(srcId)
          val dstPid = partitioner.getPartition(dstId)
          pid2Oids(srcPid).add(srcId)
          pid2Oids(dstPid).add(dstId)
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
        val time1 = System.nanoTime()
        log.info("[GraphLoader: ] iterating over edge cost " + (time1 - time0) / 1000000 + "ms")
        val res = new ArrayBuffer[(PartitionID,EdgeShuffle[Int])]
        var ind = 0
        while (ind < numEdgePartitions){
//          if (ind != fromPid){
            res.+=((ind, new EdgeShuffle(fromPid, ind, pid2Oids(ind), pid2src(ind).trim().array, pid2Dst(ind).trim().array, pid2attr(ind).trim().array)))
//          }
          ind += 1
        }
        res.toIterator
//        require(res.length == numEdgePartitions - 1)
//        log.info(s"from pid ${fromPid}, curInd ${ind}, hash value for ${fromPid} is ${partitioner.getPartition(fromPid)}")
//        EdgeShuffleToMe.set(fromPid, new EdgeShuffle[Int](fromPid, fromPid, pid2Oids(fromPid),
//          pid2src(fromPid).trim().array, pid2Dst(fromPid).trim().array, pid2attr(fromPid).trim().array))
//        val newIter = res.toIterator
//        if (newIter.isEmpty){
//          Iterator((fromPid, null.asInstanceOf[EdgeShuffle[Int]]))
//        }
//        else newIter
      }
    ).partitionBy(partitioner).persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))
    val edgeShufflesNum = edgesShuffled.count()
    val edgeShuffleTime = System.nanoTime()
    log.info(s"Repartition ${edgeShufflesNum} edges cost ${(edgeShuffleTime - linesTime)/ 1000000} ms ")

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
