package org.apache.spark.graphx

import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.graphx.impl.grape.GrapeEdgeRDDImpl
import org.apache.spark.graphx.impl.partition.{EdgeShuffle, EdgeShuffleReceived, GrapeEdgePartition}
import org.apache.spark.graphx.utils.{Constant, EdgeShuffleToMe, ExecutorUtils, GrapeEdgePartitionRegistry, VineyardClientRegistry}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import scala.reflect.ClassTag

abstract class GrapeEdgeRDD[ED](sc: SparkContext,
                                deps: Seq[Dependency[_]]) extends EdgeRDD[ED](sc, deps) {

  private[graphx] def grapePartitionsRDD: RDD[(PartitionID, GrapeEdgePartition[VD, ED])] forSome { type VD }

  override def partitionsRDD = null

  def mapValues[ED2 : ClassTag](f: Edge[ED] => ED2): GrapeEdgeRDD[ED2]

  override def innerJoin[ED2: ClassTag, ED3: ClassTag](other: EdgeRDD[ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgeRDD[ED3]

  def generateDegreeRDD(originalVertexRDD : GrapeVertexRDD[_]) : GrapeVertexRDD[Int]
}

object GrapeEdgeRDD extends Logging{
  def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]): GrapeEdgeRDD[ED] = {
    //Shuffle the edge rdd
    //then use build to build.
    null
  }

  private[graphx] def fromGrapeEdgePartitions[VD: ClassTag, ED : ClassTag](
                                                        edgePartitions: RDD[(PartitionID, GrapeEdgePartition[VD, ED])]): GrapeEdgeRDDImpl[VD, ED] = {
    //    new EdgeRDDImpl(edgePartitions)
    new GrapeEdgeRDDImpl[VD,ED](edgePartitions)
  }

  private [graphx] def initExecutorUtils[ED: ClassTag](shuffles : RDD[(PartitionID,EdgeShuffle[ED])]) : Unit = {
    shuffles.foreachPartition(iter => {
      val pid = iter.next()._1
      //It is possible next()._2 is null, see GraphLoader.scala
      ExecutorUtils.registerPartition(pid)
    })
  }

  private[graphx] def fromEdgeShuffle[VD: ClassTag, ED : ClassTag](edgeShuffles : RDD[(PartitionID, EdgeShuffle[ED])]) : GrapeEdgeRDDImpl[VD,ED] = {
    //combine edges shuffles to edge Partition
    val numPartitions = edgeShuffles.getNumPartitions
    log.info(s"edgeShuffles has ${numPartitions} parts")
    initExecutorUtils(edgeShuffles)

    val edgeShuffleReceived = edgeShuffles.mapPartitionsWithIndex((ind, iter) => {
      if (iter.hasNext) {
        val edgeShuffleReceived = new EdgeShuffleReceived[ED](numPartitions, ind)
        while (iter.hasNext) {
          val (pid, shuffle) = iter.next()
          require(pid == ind)
          if (shuffle != null){
              edgeShuffleReceived.set(shuffle.fromPid, shuffle)
	        }
        }
        //require(edgeShuffleReceived.get(ind) == null)
        edgeShuffleReceived.set(ind, EdgeShuffleToMe.get(ind).asInstanceOf[EdgeShuffle[ED]])
        log.info(s"Partition ${ind} collect received partitions ${edgeShuffleReceived}")
        Iterator((ind, edgeShuffleReceived))
      }
      else {
        Iterator.empty
      }
    })
    fromEdgeShuffleReceived(edgeShuffleReceived)
  }

  private [graphx] def fromEdgeShuffleReceived[VD: ClassTag, ED: ClassTag](edgesShuffles: RDD[(Int, EdgeShuffleReceived[ED])]) : GrapeEdgeRDDImpl[VD,ED] = {
    edgesShuffles.foreachPartition(iter => {
      val (pid, edges) = iter.next()
      val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
      registry.addEdgesToBuilder(pid, edges)
    })
    log.info("[GrapeEdgeRDD]: Finish adding edges to builder")

    edgesShuffles.foreachPartition(iter => {
      val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
      registry.buildLocalVertexMap(iter.next()._1)
    })
    log.info("[GrapeEdgeRDD]: Finish building local vmId")

    val localVMIDs = edgesShuffles.mapPartitions(iter => {
      Iterator(ExecutorUtils.getHost2LocalVMID)
    }).collect().distinct.sorted // sort the ids with respect to hostnames.
    log.info(s"[GrapeEdgeRDD]: got distinct local vm ids ${localVMIDs.mkString("Array(", ", ", ")")}")

    log.info("[GrapeEdgeRDD]: Start constructing global vm")
    val globalVMIDs = MPIUtils.constructGlobalVM(localVMIDs.mkString(","), Constant.vineyardEndpoint, "int64_t", "uint64_t")
    log.info(s"[GrapeEdgeRDD]: Finish constructing global vm ${globalVMIDs}")
    edgesShuffles.foreachPartition(iter => {
      ExecutorUtils.setGlobalVMIDs(globalVMIDs)
    })

    edgesShuffles.foreachPartition(iter => {
      val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
      registry.buildCSR(iter.next()._1)
    })

    val grapeEdgePartitions = edgesShuffles.mapPartitions(iter => {
      val (pid, part) = iter.next()
      val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
      Iterator((pid,registry.getEdgePartition(pid)))
    }).cache()

    val rdd =new GrapeEdgeRDDImpl[VD,ED](grapeEdgePartitions)
    log.info(s"[GrapeEdgeRDD:] Finish Construct EdgeRDD, total edges count ${rdd.count()}")
    rdd
  }

//  private[graphx] def fromEdgePartitions[VD: ClassTag, ED : ClassTag](
//                                                          edgePartitions: RDD[(PartitionID, EdgePartition[ED, VD])]): GrapeEdgeRDDImpl[VD, ED] = {
//    //1. edgePartition to memory mapped file.
//    val totalNumEdges = edgePartitions.map(_._2.size.toLong).fold(0)(_ + _)
//    log.info(s"Driver: Total num edges in Partition: ${totalNumEdges}")
//    val edgeMappedSize = 32L * totalNumEdges  + 128
//    //FIXME: no shared memory need. use byteVectorStream
////    val outputFilenames = SharedMemoryUtils.mapEdgePartitionToFile(edgePartitions, "graphx-edge", edgeMappedSize);
//
////    val outputFilenamesDedup = dedup(outputFilenames).mkString(":")
////    log.info(s"[Driver: ] got mapped edge files ${outputFilenamesDedup}")
//
//    edgePartitions.foreachPartition(iter => {
//      val (pid, part) = iter.next()
//      lazy val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
////      registry.registerPath(pid, outputFilenamesDedup)
//      registry.createArrayBuilder(pid)
//    })
//    log.info(s"[Driver:] Finish create array Builder")
//
//    edgePartitions.foreachPartition(iter => {
//      val (pid, part) = iter.next()
//      lazy val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
//      val (srcBuilder,dstBuilder,edataBuilder) = registry.getBuilders()
//      srcBuilder.reserve(part.size)
//      dstBuilder.reserve(part.size)
//      edataBuilder.reserve(part.size)
//      val partIter = part.iterator
//      while (partIter.hasNext){
//        val edge = partIter.next()
//        srcBuilder.unsafeAppend(edge.srcId)
//        dstBuilder.unsafeAppend(edge.dstId)
//        edataBuilder.unsafeAppend(edge.attr)
//      }
//      log.info("Finish build srcOid array");
//    })
//
//    edgePartitions.foreachPartition(iter => {
//      val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
//      registry.constructEdgePartition(iter.next()._1)
//    })
//
//    log.info(s"[Driver:] Finish construct edge partition")
//
//    val grapeEdgePartitionWrapper = edgePartitions.mapPartitions(iter => {
//      val (pid, part) = iter.next()
//      val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
//      Iterator((pid,registry.getEdgePartitionWrapper(pid)))
//    }).cache()
//    //Clear registry
//
//    val rdd =new GrapeEdgeRDDImpl[VD,ED](grapeEdgePartitionWrapper)
//    log.info(s"[Driver:] got grape edge Partition Wrapper, total edges count ${rdd.count()}")
//
//    // edgePartitions.foreachPartition(_ =>  GrapeEdgePartitionRegistry.clear())
//    rdd
//  }


}
