package org.apache.spark.graphx

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.graphx.impl.grape.GrapeEdgeRDDImpl
import org.apache.spark.graphx.impl.partition.{EdgeShuffle, EdgeShuffleReceived, GrapeEdgePartition, GrapeEdgePartitionBuilder}
import org.apache.spark.graphx.utils.{Constant, ExecutorUtils, GrapeMeta, ScalaFFIFactory}
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

  def generateDegreeRDD(originalVertexRDD : GrapeVertexRDD[_], edgeDirection : EdgeDirection) : GrapeVertexRDD[Int]
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

  private[graphx] def fromEdgeShuffle[VD: ClassTag, ED : ClassTag](edgeShuffles : RDD[(PartitionID, EdgeShuffle[ED])]) : GrapeEdgeRDDImpl[VD,ED] = {
    //combine edges shuffles to edge Partition
    val numPartitions = edgeShuffles.getNumPartitions
    log.info(s"edgeShuffles has ${numPartitions} parts")

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
        log.info(s"Partition ${ind} collect received partitions ${edgeShuffleReceived}")
        Iterator((ind, edgeShuffleReceived))
      }
      else {
        Iterator.empty
      }
    }).cache()
    fromEdgeShuffleReceived(edgeShuffleReceived)
  }

  private [graphx] def fromEdgeShuffleReceived[VD: ClassTag, ED: ClassTag](edgesShuffles: RDD[(Int, EdgeShuffleReceived[ED])]) : GrapeEdgeRDDImpl[VD,ED] = {
    val numPartitions = edgesShuffles.getNumPartitions
    val edgesShuffleWithMeta = edgesShuffles.mapPartitionsWithIndex((pid, iter) => {
      if (iter.hasNext){
        val vineyardClient = ScalaFFIFactory.newVineyardClient()
        val ffiByteString = FFITypeFactory.newByteString()
        ffiByteString.copyFrom(Constant.vineyardEndpoint)
        vineyardClient.connect(ffiByteString)
        val (_pid, shuffleReceived) = iter.next()
        require(pid == _pid, s"not possible ${pid}, ${_pid}")
        val grapeMeta = new GrapeMeta[VD,ED](pid, numPartitions,vineyardClient,ExecutorUtils.getHostName)
        Iterator((grapeMeta, shuffleReceived))
      }
      else Iterator.empty
    },true).cache()

    val localVertexMapIdss = edgesShuffleWithMeta.mapPartitions(iter => {
      if (iter.hasNext){
        val (meta, shufflesReceived) = iter.next()
        val edgePartitionBuilder = new GrapeEdgePartitionBuilder[VD,ED](meta.vineyardClient)
        edgePartitionBuilder.addEdges(shufflesReceived)
        val localVertexMap = edgePartitionBuilder.buildLocalVertexMap()
        meta.setLocalVertexMap(localVertexMap)
        meta.setEdgePartitionBuilder(edgePartitionBuilder)
        Iterator(ExecutorUtils.getHostName + ":" + meta.partitionID + ":" + localVertexMap.id())
      }
      else Iterator.empty
    }).collect().distinct.sorted

    log.info(s"[GrapeEdgeRDD]: got distinct local vm ids ${localVertexMapIdss.mkString("Array(", ", ", ")")}")
    require(localVertexMapIdss.length == numPartitions, s"${localVertexMapIdss.length} neq to num partitoins ${numPartitions}")

    log.info("[GrapeEdgeRDD]: Start constructing global vm")
    val globalVMIDs = MPIUtils.constructGlobalVM(localVertexMapIdss, Constant.vineyardEndpoint, "int64_t", "uint64_t")
    log.info(s"[GrapeEdgeRDD]: Finish constructing global vm ${globalVMIDs}")
    require(globalVMIDs.size() == numPartitions)

    val metaUpdated = edgesShuffleWithMeta.mapPartitions(iter => {
      if (iter.hasNext) {
        var i = 0
        val hostName = ExecutorUtils.getHostName
        val (meta, part) = iter.next()
        var res = null.asInstanceOf[String]
        while (i < globalVMIDs.size()) {
          val ind = globalVMIDs.get(i).indexOf(hostName)
          if (ind != -1) {
            val spltted = globalVMIDs.get(i).split(":")
            require(spltted.length == 3) // hostname,pid,vmid
            require(spltted(0) == hostName)
            if (spltted(1).toInt == meta.partitionID) {
              res = spltted(2)
            }
          }
          i += 1
        }
        require(res != null, s"after iterate over received global ids, no suitable found for ${meta.partitionID} : ${globalVMIDs}")
        meta.setGlobalVM(res.toLong)
        Iterator((meta, part))
      }
      else Iterator.empty
    }).cache()

    val metaUpdated2 = metaUpdated.mapPartitions(iter => {
      if (iter.hasNext) {
        val (meta,part) = iter.next()
        require(meta.globalVMId != -1)
        val (vm, csr) = meta.edgePartitionBuilder.buildCSR(meta.globalVMId)
        meta.setGlobalVM(vm)
        meta.setCSR(csr)
        Iterator((meta, part))
      }
      else Iterator.empty
    }).cache()

    val grapeEdgePartitions = metaUpdated2.mapPartitions(iter => {
      if (iter.hasNext) {
        val (meta, part) = iter.next()
        Iterator((meta.partitionID, new GrapeEdgePartition[VD, ED](meta.partitionID, meta.graphxCSR, meta.globalVM, meta.vineyardClient)))
      }
      else Iterator.empty
    }).cache()

    val rdd =new GrapeEdgeRDDImpl[VD,ED](grapeEdgePartitions)
    log.info(s"[GrapeEdgeRDD:] Finish Construct EdgeRDD, total edges count ${rdd.count()}")
    rdd
  }

}
