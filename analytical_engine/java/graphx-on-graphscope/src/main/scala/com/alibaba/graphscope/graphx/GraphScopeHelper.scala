package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.utils.{LongLong, LongLongInputFormat}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.graphx.{EdgeRDD, GrapeEdgeRDD, GrapeVertexRDD, Graph, PartitionID, TypeAlias, VertexId, VertexRDD}
import org.apache.spark.graphx.impl.{EdgeRDDImpl, GrapeGraphImpl, GraphImpl, VertexRDDImpl}
import org.apache.spark.graphx.impl.partition.EdgeShuffle
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{OpenHashSet, PrimitiveVector}
import org.apache.spark.{HashPartitioner, SparkContext}

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object GraphScopeHelper extends Logging{
  /**
   * Creating GSSession, one spark context can have many graphscope session.
   */
  def createSession(sc : SparkContext) : GSSession = {
    new GSSession(sc)
  }

  def edgeListFile
  (sc: SparkContext,
   path: String,
   canonicalOrientation: Boolean = false,
   numPartitions: Int = -1,
   edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
   vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : Graph[Int, Int] = {
    val startTimeNs = System.nanoTime()
    // Parse the edge data table directly into edge partitions
    val lines = {
      if (numPartitions > 0) {
        sc.hadoopFile(path, classOf[LongLongInputFormat], classOf[LongWritable],classOf[LongLong]).coalesce(numPartitions).setName(path)
      } else {
        sc.hadoopFile(path, classOf[LongLongInputFormat], classOf[LongWritable],classOf[LongLong]).setName(path)
      }
    }.map(pair => (pair._2.first, pair._2.second))
    lines.cache()
    val linesTime = System.nanoTime()
    log.info("[edgeListFile]: load partitions cost " + (linesTime - startTimeNs) / 1000000 + "ms")
    //    val numLines = lines.count() / numPartitions
    val partitioner = new HashPartitioner(numPartitions)
    val edgesShuffled = lines.mapPartitionsWithIndex (
      (fromPid, iter) => {
        //        iter.toArray
        val pid2src = Array.fill(numPartitions)(new TypeAlias.PrimitiveVector[VertexId])
        val pid2Dst = Array.fill(numPartitions)(new TypeAlias.PrimitiveVector[VertexId])
        val pid2attr = Array.fill(numPartitions)(new TypeAlias.PrimitiveVector[Int])
        val pid2Oids = Array.fill(numPartitions)(new OpenHashSet[VertexId])
        val time0 = System.nanoTime();
        while (iter.hasNext) {
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
        log.info("[edgeListFile: ] iterating over edge cost " + (time1 - time0) / 1000000 + "ms")
        val res = new ArrayBuffer[(PartitionID,EdgeShuffle[Int])]
        var ind = 0
        while (ind < numPartitions){
          log.info(s"partition ${fromPid} send msg to ${ind}")
          res.+=((ind, new EdgeShuffle(fromPid, ind, pid2Oids(ind), pid2src(ind).trim().array, pid2Dst(ind).trim().array, pid2attr(ind).trim().array)))
          ind += 1
        }
        res.toIterator
      }
    ).partitionBy(partitioner).setName("GraphScopeHelper.edgeListFile - edges (%s)".format(path))
    val edgeShufflesNum = edgesShuffled.count()
    val edgeShuffleTime = System.nanoTime()
    log.info(s"Repartition ${edgeShufflesNum} edges cost ${(edgeShuffleTime - linesTime)/ 1000000} ms ")

    logInfo(s"It took ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms" +
      " to load the edges")

    val time0 = System.nanoTime()
    val edgeRDD = GrapeEdgeRDD.fromEdgeShuffle[Int,Int](edgesShuffled).cache()
    val time1 = System.nanoTime()
    log.info(s"[edgeListFile:] construct edge rdd ${edgeRDD} cost ${(time1 - time0) / 1000000} ms")
    val vertexRDD = GrapeVertexRDD.fromGrapeEdgeRDD[Int](edgeRDD, edgeRDD.grapePartitionsRDD.getNumPartitions, 1,vertexStorageLevel).cache()
    log.info(s"num vertices ${vertexRDD.count()}, num edges ${edgeRDD.count()}")
    lines.unpersist()
    GrapeGraphImpl.fromExistingRDDs(vertexRDD,edgeRDD)
  }

  def rdd2Fragment[VD : ClassTag,ED : ClassTag](vertexRDD : VertexRDD[VD], edgeRDD : EdgeRDD[ED]) : GrapeGraphImpl[VD,ED] = {
    if (vertexRDD.isInstanceOf[GrapeVertexRDD[VD]] && edgeRDD.isInstanceOf[GrapeEdgeRDD[ED]]){
      val grapeGraph = GrapeGraphImpl.fromExistingRDDs(vertexRDD.asInstanceOf[GrapeVertexRDD[VD]],edgeRDD.asInstanceOf[GrapeEdgeRDD[ED]])
      graph2Fragment(grapeGraph)
    }
    else if (vertexRDD.isInstanceOf[VertexRDDImpl[VD]] && edgeRDD.isInstanceOf[EdgeRDDImpl[ED,VD]]){
      val graph = GraphImpl.fromExistingRDDs(vertexRDD,edgeRDD)
      graph2Fragment(graph)
    }
    else {
      log.error(s"Unable to convert ${vertexRDD} and ${edgeRDD} to grape fragment")
      null
    }
  }

  /** Convert a common graphX graph to grape graph, which later can be used to run GraphScope app. */
  def graph2Fragment[VD: ClassTag,ED : ClassTag](graph : Graph[VD,ED]) : GrapeGraphImpl[VD,ED] = {
    //If input graph is already instance of grapeGraphImpl, we can just return. Although its vdata
    //and edata may be changed and stored in java-heap, we do not hurry to persist data to c++.
    //We only invoke data persistence when this graph is input to sess.run(cmd, InputGraph) or graph.pregel(...).
    graph match {
      case value: GrapeGraphImpl[VD, ED] =>
        value
      case value : GraphImpl[VD,ED] =>
        graphxGraph2Fragment(value)
      case _ => throw new IllegalStateException(s"Not recognized graph ${graph}")
    }
  }
  private [graphx] def graphxGraph2Fragment[VD: ClassTag,ED : ClassTag](originGraph: GraphImpl[VD, ED]) : GrapeGraphImpl[VD,ED] = {
    val numPartitions = originGraph.vertices.getNumPartitions
    val partitioner = new HashPartitioner(numPartitions)
    val time0 = System.nanoTime()
    //create graph partition from edges.
    val originEdgePartitions = originGraph.edges.partitionsRDD
    val edgesShuffled = originEdgePartitions.mapPartitions(iter => {
      val (fromPid, part) = iter.next()
      val pid2src = Array.fill(numPartitions)(new TypeAlias.PrimitiveVector[VertexId])
      val pid2Dst = Array.fill(numPartitions)(new TypeAlias.PrimitiveVector[VertexId])
      val pid2attr = Array.fill(numPartitions)(new TypeAlias.PrimitiveVector[ED])
      val pid2Oids = Array.fill(numPartitions)(new OpenHashSet[VertexId])
      val time0 = System.nanoTime()
      val edgesIter = part.iterator
      while (edgesIter.hasNext) {
        val edge = edgesIter.next()
        val srcId = edge.srcId
        val dstId = edge.dstId
        val srcPid = partitioner.getPartition(srcId)
        val dstPid = partitioner.getPartition(dstId)
        pid2Oids(srcPid).add(srcId)
        pid2Oids(dstPid).add(dstId)
        if (srcPid == dstPid){
          pid2src(srcPid).+=(srcId)
          pid2Dst(srcPid).+=(dstId)
          pid2attr(srcPid)+=(edge.attr)
        }
        else {
          pid2src(srcPid).+=(srcId)
          pid2Dst(srcPid).+=(dstId)
          pid2attr(srcPid).+=(edge.attr)
          pid2src(dstPid).+=(srcId)
          pid2Dst(dstPid).+=(dstId)
          pid2attr(dstPid).+=(edge.attr)
        }
      }
      val time1 = System.nanoTime()
      log.info("[GraphxGraph2Fragment: ] iterating over edge cost " + (time1 - time0) / 1000000 + "ms")
      val res = new ArrayBuffer[(PartitionID,EdgeShuffle[ED])]
      var ind = 0
      while (ind < numPartitions){
        log.info(s"partition ${fromPid} send msg to ${ind}")
        res.+=((ind, new EdgeShuffle(fromPid, ind, pid2Oids(ind), pid2src(ind).trim().array, pid2Dst(ind).trim().array, pid2attr(ind).trim().array)))
        ind += 1
      }
      res.toIterator
    }).partitionBy(partitioner).setName("GraphxGraph2Fragment.edgeListFile")
    val edgeShufflesNum = edgesShuffled.count()
    val time1 = System.nanoTime()
    logInfo(s"It took ${TimeUnit.NANOSECONDS.toMillis(time1 - time0)} ms" +
      s" to load the edges,shuffle count ${edgeShufflesNum}")

    val time2 = System.nanoTime()
    val edgeRDD = GrapeEdgeRDD.fromEdgeShuffle[VD,ED](edgesShuffled).cache()
    val time3 = System.nanoTime()
    //different from edgeFileLoader, here we need the attr in the original graphx graph
    val verticesShuffled = originGraph.vertices.mapPartitions(iter => {
      val pid2Oid = Array.fill(numPartitions)(new TypeAlias.PrimitiveVector[Long])
      val pid2Vd = Array.fill(numPartitions)(new TypeAlias.PrimitiveVector[VD])
      while (iter.hasNext){
        val (id, vd) = iter.next()
        val pid = partitioner.getPartition(id)
        pid2Oid(pid).+=(id)
        pid2Vd(pid).+=(vd)
      }
      val res = new ArrayBuffer[(PartitionID,(Array[Long],Array[VD]))]()
      var ind = 0
      while (ind < numPartitions){
        res.+=((ind, (pid2Oid(ind).trim().array, pid2Vd(ind).trim().array)))
        ind += 1
      }
      res.toIterator
    }).partitionBy(partitioner).setName("GraphxGraph2Fragment.vertices")

    log.info(s"[GraphxGraph2Fragment:] construct edge rdd ${edgeRDD} cost ${(time3 - time2) / 1000000} ms")
    val vertexRDD = GrapeVertexRDD.fromGrapeEdgeRDDAndGraphXVertexRDD[VD](edgeRDD,verticesShuffled, numPartitions, StorageLevel.MEMORY_ONLY).cache()
    log.info(s"num vertices ${vertexRDD.count()}, num edges ${edgeRDD.count()}")
    edgesShuffled.unpersist()
    GrapeGraphImpl.fromExistingRDDs(vertexRDD,edgeRDD)
  }
}
