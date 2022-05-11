package org.apache.spark.graphx

import org.apache.spark.graphx.impl.GrapeUtils.dedup
import org.apache.spark.graphx.impl.{EdgePartition, GrapeEdgePartitionWrapper}
import org.apache.spark.graphx.impl.grape.GrapeEdgeRDDImpl
import org.apache.spark.graphx.impl.partition.{EdgeShuffle, EdgeShuffleReceived}
import org.apache.spark.graphx.utils.{GrapeEdgePartitionRegistry, SharedMemoryUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import scala.reflect.ClassTag

abstract class GrapeEdgeRDD[ED](sc: SparkContext,
                                deps: Seq[Dependency[_]]) extends EdgeRDD[ED](sc, deps) {

  private[graphx] def grapePartitionsRDD: RDD[(PartitionID, GrapeEdgePartitionWrapper[VD, ED])] forSome { type VD }

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
                                                        edgePartitions: RDD[(PartitionID, GrapeEdgePartitionWrapper[VD, ED])]): GrapeEdgeRDDImpl[VD, ED] = {
    //    new EdgeRDDImpl(edgePartitions)
    new GrapeEdgeRDDImpl[VD,ED](edgePartitions)
  }

  private[graphx] def fromEdgeShuffle[VD: ClassTag, ED : ClassTag](edgeShuffles : RDD[(PartitionID, EdgeShuffle[ED])]) : GrapeEdgeRDDImpl[VD,ED] = {
    //combine edges shuffles to edge Partition
    val numPartitions = edgeShuffles.getNumPartitions
    log.info(s"edgeShuffles has ${numPartitions} parts")
    val edgeShuffleReceived = edgeShuffles.mapPartitionsWithIndex((ind,iter) => {
      if (iter.hasNext){
        val edgeShuffleReceived = new EdgeShuffleReceived[ED](numPartitions, ind)
        while (iter.hasNext){
          val (pid, shuffle) = iter.next()
          require(pid == ind)
          edgeShuffleReceived.set(shuffle.fromPid, shuffle)
        }
        log.info(s"Partition ${ind} collect received partitions ${edgeShuffleReceived}")
        Iterator((ind, edgeShuffleReceived))
      }
      else {
        Iterator.empty
      }
    })

    val builders = edgeShuffleReceived.mapPartitions(iter => {
      val (pid, part) = iter.next()
      val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
      val builders = registry.createArrayBuilder(pid, part.totalSize())
      Iterator((pid, builders))
    })
    log.info(s"[Driver:] Finish create array Builder")

    val newBuilders = edgeShuffleReceived.zipPartitions(builders)( (edgeShuffleIter, builderIter) => {
      val (pid, edgeShuffle) = edgeShuffleIter.next()
      val (pid2, builders) = builderIter.next()
      require(pid == pid2)
      edgeShuffle.feedToBuilder(builders._1, builders._2, builders._3)
      Iterator((pid2, builders))
    })

    newBuilders.foreachPartition(iter => {
      val (pid, builders) = iter.next()
      val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
      registry.constructEdgePartition(pid, builders._1, builders._2, builders._3)
    })

    log.info(s"[Driver:] Finish construct edge partition")

    val grapeEdgePartitionWrapper = edgeShuffles.mapPartitions(iter => {
      val (pid, part) = iter.next()
      val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
      Iterator((pid,registry.getEdgePartitionWrapper(pid)))
    }).cache()

    val rdd =new GrapeEdgeRDDImpl[VD,ED](grapeEdgePartitionWrapper)
    log.info(s"[Driver:] got grape edge Partition Wrapper, total edges count ${rdd.count()}")
    rdd
  }

  private[graphx] def fromEdgePartitions[VD: ClassTag, ED : ClassTag](
                                                          edgePartitions: RDD[(PartitionID, EdgePartition[ED, VD])]): GrapeEdgeRDDImpl[VD, ED] = {
    //1. edgePartition to memory mapped file.
    val totalNumEdges = edgePartitions.map(_._2.size.toLong).fold(0)(_ + _)
    log.info(s"Driver: Total num edges in Partition: ${totalNumEdges}")
    val edgeMappedSize = 32L * totalNumEdges  + 128
    //FIXME: no shared memory need. use byteVectorStream
//    val outputFilenames = SharedMemoryUtils.mapEdgePartitionToFile(edgePartitions, "graphx-edge", edgeMappedSize);

//    val outputFilenamesDedup = dedup(outputFilenames).mkString(":")
//    log.info(s"[Driver: ] got mapped edge files ${outputFilenamesDedup}")

    edgePartitions.foreachPartition(iter => {
      val (pid, part) = iter.next()
      lazy val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
//      registry.registerPath(pid, outputFilenamesDedup)
      registry.createArrayBuilder(pid)
    })
    log.info(s"[Driver:] Finish create array Builder")

    edgePartitions.foreachPartition(iter => {
      val (pid, part) = iter.next()
      lazy val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
      val (srcBuilder,dstBuilder,edataBuilder) = registry.getBuilders()
      srcBuilder.reserve(part.size)
      dstBuilder.reserve(part.size)
      edataBuilder.reserve(part.size)
      val partIter = part.iterator
      while (partIter.hasNext){
        val edge = partIter.next()
        srcBuilder.unsafeAppend(edge.srcId)
        dstBuilder.unsafeAppend(edge.dstId)
        edataBuilder.unsafeAppend(edge.attr)
      }
      log.info("Finish build srcOid array");
    })

    edgePartitions.foreachPartition(iter => {
      val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
      registry.constructEdgePartition(iter.next()._1)
    })

    log.info(s"[Driver:] Finish construct edge partition")

    val grapeEdgePartitionWrapper = edgePartitions.mapPartitions(iter => {
      val (pid, part) = iter.next()
      val registry = GrapeEdgePartitionRegistry.getOrCreate[VD,ED]
      Iterator((pid,registry.getEdgePartitionWrapper(pid)))
    }).cache()
    //Clear registry

    val rdd =new GrapeEdgeRDDImpl[VD,ED](grapeEdgePartitionWrapper)
    log.info(s"[Driver:] got grape edge Partition Wrapper, total edges count ${rdd.count()}")

    // edgePartitions.foreachPartition(_ =>  GrapeEdgePartitionRegistry.clear())
    rdd

  }


}
