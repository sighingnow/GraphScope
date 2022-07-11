package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.format.{LongLong, LongLongInputFormat}
import com.alibaba.graphscope.fragment.ArrowProjectedFragment
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes
import com.alibaba.graphscope.graphx.graph.impl.FragmentStructure
import com.alibaba.graphscope.graphx.rdd.FragmentRDD
import com.alibaba.graphscope.graphx.shuffle.EdgeShuffle
import com.alibaba.graphscope.graphx.store.VertexDataStore
import com.alibaba.graphscope.graphx.utils.{GrapeUtils, PrimitiveVector, ScalaFFIFactory}
import com.alibaba.graphscope.utils.GenericUtils
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.hadoop.io.LongWritable
import org.apache.spark.graphx.grape.{GrapeEdgeRDD, GrapeGraphImpl, GrapeVertexRDD, PartitionAwareZippedBaseRDD}
import org.apache.spark.graphx.impl.{GraphImpl, VertexRDDImpl}
import org.apache.spark.graphx.scheduler.cluster.ExecutorInfoHelper
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.{HashPartitioner, SparkContext}

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object GraphScopeHelper extends Logging{

  def loadFragmentAsRDD[VD: ClassTag, ED: ClassTag](sc : SparkContext, objectIDs : String, fragName : String) : (GrapeVertexRDD[VD],GrapeEdgeRDD[ED]) = {

    val fragmentRDD = new FragmentRDD[VD,ED](sc, ExecutorInfoHelper.getExecutors(sc), fragName,objectIDs)
    fragmentRDD.generateRDD()
  }

  def loadFragmentAsGraph[VD: ClassTag, ED: ClassTag](sc : SparkContext, objectIDs : String, fragName : String) : GrapeGraphImpl[VD,ED] = {
    val (vertexRDD,edgeRDD) = loadFragmentAsRDD[VD,ED](sc, objectIDs, fragName);
    GrapeGraphImpl.fromExistingRDDs[VD,ED](vertexRDD,edgeRDD)
  }
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
    // Parse the edge data table directly into edge partitions
    val lines = {
      if (numPartitions > 0) {
        sc.hadoopFile(path, classOf[LongLongInputFormat], classOf[LongWritable],classOf[LongLong]).setName(path) //coalesce(fakeNumPartitions)
      } else {
        sc.hadoopFile(path, classOf[LongLongInputFormat], classOf[LongWritable],classOf[LongLong]).setName(path)
      }
    }.map(pair => (pair._2.first, pair._2.second))
    val linesTime = System.nanoTime()
    //    val numLines = lines.count() / numPartitions
    val partitioner = new HashPartitioner(numPartitions)
    val edgesShuffled = lines.mapPartitionsWithIndex (
      (fromPid, iter) => {
        //        iter.toArray
        val pid2src = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
        val pid2Dst = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
        val pid2Oids = Array.fill(numPartitions)(new OpenHashSet[VertexId])
        val pid2OuterIds = Array.fill(numPartitions)(new OpenHashSet[VertexId])
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
          }
          else {
            pid2src(srcPid).+=(srcId)
            pid2Dst(srcPid).+=(dstId)
            pid2OuterIds(srcPid).add(dstId)
            pid2src(dstPid).+=(srcId)
            pid2Dst(dstPid).+=(dstId)
            pid2OuterIds(dstPid).add(srcId)
          }
        }
        val time1 = System.nanoTime()
        log.info("[edgeListFile: ] iterating over edge cost " + (time1 - time0) / 1000000 + "ms")
        val res = new ArrayBuffer[(PartitionID,EdgeShuffle[Int,Int])]
        var ind = 0
        while (ind < numPartitions){
//          log.info(s"partition ${fromPid} send msg to ${ind}")
          res.+=((ind, new EdgeShuffle(fromPid, ind, pid2Oids(ind),pid2OuterIds(ind), pid2src(ind).trim().array, pid2Dst(ind).trim().array)))
          ind += 1
        }
        res.toIterator
      }
    ).partitionBy(partitioner).setName("GraphScopeHelper.edgeListFile - edges (%s)".format(path)).cache()
    val edgeShufflesNum = edgesShuffled.count()

    logInfo(s"It took ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - linesTime)} ms" +
      s" to load the edges size ${edgeShufflesNum}")

    val time0 = System.nanoTime()
    val edgeRDD = GrapeEdgeRDD.fromEdgeShuffle[Int,Int](edgesShuffled,defaultED = 1).cache()
    val vertexRDD = GrapeVertexRDD.fromGrapeEdgeRDD[Int](edgeRDD, edgeRDD.grapePartitionsRDD.getNumPartitions, 1,vertexStorageLevel).cache()
    log.info(s"num vertices ${vertexRDD.count()}, num edges ${edgeRDD.count()}")
    lines.unpersist()
    val time1 = System.nanoTime()
    log.info(s"[edgeListFile:] construct grape edge rdd ${edgeRDD.count()} and vertex rdd ${vertexRDD.count()} cost ${(time1 - time0) / 1000000} ms")
    GrapeGraphImpl.fromExistingRDDs(vertexRDD,edgeRDD)
  }

  def rdd2Fragment[VD : ClassTag,ED : ClassTag](vertexRDD : VertexRDD[VD], edgeRDD : EdgeRDD[ED]) : GrapeGraphImpl[VD,ED] = {
    vertexRDD match {
      case grapeVertexRDD: GrapeVertexRDD[VD] =>
        val grapeGraph = GrapeGraphImpl.fromExistingRDDs(grapeVertexRDD, edgeRDD.asInstanceOf[GrapeEdgeRDD[ED]])
        graph2Fragment(grapeGraph)
      case vertexRDDImpl : VertexRDDImpl[VD] =>
        val graph = GraphImpl.fromExistingRDDs(vertexRDD, edgeRDD)
        graph2Fragment(graph)
      case _ =>
        log.error(s"Unable to convert ${vertexRDD} and ${edgeRDD} to grape fragment")
        null
    }
  }

  /**
   * Given a fragment-backend graph, we writeback the modified data to original fragment,
   * resulting a new graph.
   */
  def writeBackFragment[VD : ClassTag, ED : ClassTag](graph : GrapeGraphImpl[VD,ED]) : Array[String] = {
    graph.backend match {
      case GraphStructureTypes.GraphXFragmentStructure =>
        log.info(s"Write back graphx fragment for ${graph}")
        val res = graph.fragmentIds.collect()
        res.map(str => str.substring(0,str.indexOf(":")) + ":" + str.substring(str.lastIndexOf(":") + 1))
      case GraphStructureTypes.ArrowProjectedStructure =>
        log.info(s"Write back projected fragment for ${graph}")
        val grapeVerticesPartitions = graph.grapeVertices.grapePartitionsRDD
        val res = PartitionAwareZippedBaseRDD.zipPartitions(SparkContext.getOrCreate(), graph.grapeEdges.grapePartitionsRDD, grapeVerticesPartitions){
          (edgeIter, vertexIter) => {
            val edgePart = edgeIter.next()
            val vertexPart = vertexIter.next()
            val structure = edgePart.graphStructure.asInstanceOf[FragmentStructure]
            val oldProjectedFrag = structure.fragment.asInstanceOf[ArrowProjectedAdaptor[Long,Long,_,_]].getArrowProjectedFragment.asInstanceOf[ArrowProjectedFragment[Long,Long,_,_]]
            //get the original fragment type parameters
            val typeParams = GenericUtils.getTypeArgumentFromInterface(oldProjectedFrag.getClass)
            require(typeParams.length == 4)
            val (vdClass,edClass) = (typeParams(2), typeParams(3))
            log.info(s"Original vd class ${vdClass} ed class ${edClass}")
            //create mapper
            val resFrag = doMap(oldProjectedFrag.asInstanceOf[ArrowProjectedFragment[Long,Long,Any,Any]],vdClass.asInstanceOf[Class[Any]], edClass.asInstanceOf[Class[Any]], vertexPart.vertexData, edgePart.edatas, edgePart.client)
            Iterator(GrapeUtils.getSelfHostName + ":" + resFrag.id())
          }
        }
        res.collect()
      case _ =>
        throw new IllegalStateException("Not recognized structure")
    }
  }

  def doMap[NEW_VD : ClassTag,NEW_ED : ClassTag, OLD_VD <: Any, OLD_ED <: Any](oldFrag: ArrowProjectedFragment[Long, Long, OLD_VD, OLD_ED],
                                                 oldVdClass : Class[OLD_VD], oldEdClass : Class[OLD_ED],
                                                 vdArray : VertexDataStore[NEW_VD],
                                                 edArray : Array[NEW_ED],
                                                 client : VineyardClient):
  ArrowProjectedFragment[Long,Long,NEW_VD,NEW_ED] = {
    val mapper = ScalaFFIFactory.newProjectedFragmentMapper[NEW_VD,NEW_ED,OLD_VD,OLD_ED](oldVdClass,oldEdClass)
    val newVdBuilder : ArrowArrayBuilder[NEW_VD] = ScalaFFIFactory.newArrowArrayBuilder[NEW_VD](GrapeUtils.getRuntimeClass[NEW_VD].asInstanceOf[Class[NEW_VD]])
    val newEdBuilder : ArrowArrayBuilder[NEW_ED] = ScalaFFIFactory.newArrowArrayBuilder[NEW_ED](GrapeUtils.getRuntimeClass[NEW_ED].asInstanceOf[Class[NEW_ED]])
    //Filling vd array
    val ivnum = oldFrag.getInnerVerticesNum
    newVdBuilder.reserve(ivnum)
    var i = 0;
    while (i < ivnum){
      newVdBuilder.unsafeAppend(vdArray.getData(i))
      i += 1
    }
    val enum = edArray.length
    newEdBuilder.reserve(enum)
    i = 0
    while (i < enum){
      newEdBuilder.unsafeAppend(edArray(i))
      i += 1
    }
    mapper.map(oldFrag,newVdBuilder, newEdBuilder,client).get()
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
    //Note: Before convert to graphx graph, we need to get edges and vertices attr on two endpoint.
    //We can not separate the shuffling stage into to distinct ones. Because the result partitions
    //has inconsistent pids.

    val graphShuffles = GrapeGraphImpl.generateGraphShuffle(originGraph,partitioner)

    val edgeShufflesNum = graphShuffles.count()
    val time1 = System.nanoTime()
    logInfo(s"It took ${TimeUnit.NANOSECONDS.toMillis(time1 - time0)} ms" +
      s" to load the edges,shuffle count ${edgeShufflesNum}")

    val time2 = System.nanoTime()
    val edgeRDD = GrapeEdgeRDD.fromEdgeShuffle[VD,ED](graphShuffles).cache()

    //different from edgeFileLoader, here we need the attr in the original graphx graph
    val vertexRDD = GrapeVertexRDD.fromEdgeShuffle[VD,ED](graphShuffles,edgeRDD).cache()

    val time3 = System.nanoTime()
    log.info(s"[GraphScopeHelper:] construct vertex and edge rdd ${edgeRDD} cost ${(time3 - time2) / 1000000} ms")
    log.info(s"num vertices ${vertexRDD.count()}, num edges ${edgeRDD.count()}")
    graphShuffles.unpersist()
    GrapeGraphImpl.fromExistingRDDs(vertexRDD,edgeRDD)
  }
}
