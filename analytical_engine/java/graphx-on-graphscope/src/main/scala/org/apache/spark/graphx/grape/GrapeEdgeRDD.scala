package org.apache.spark.graphx.grape

import com.alibaba.graphscope.ds.{ImmutableTypedArray, Vertex}
import com.alibaba.graphscope.graphx.graph.impl.GraphXGraphStructure
import com.alibaba.graphscope.graphx.rdd.VineyardRDD
import com.alibaba.graphscope.graphx.rdd.impl.{GrapeEdgePartition, GrapeEdgePartitionBuilder}
import com.alibaba.graphscope.graphx.shuffle.{EdgeShuffle, EdgeShuffleReceived}
import com.alibaba.graphscope.graphx.utils.{ExecutorUtils, GrapeMeta}
import com.alibaba.graphscope.utils.{FFITypeFactoryhelper, MPIUtils}
import org.apache.spark.graphx.grape.impl.GrapeEdgeRDDImpl
import org.apache.spark.graphx.scheduler.cluster.ExecutorInfoHelper
import org.apache.spark.graphx.{Edge, EdgeRDD, PartitionID, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import java.net.InetAddress
import scala.reflect.ClassTag

abstract class GrapeEdgeRDD[ED](sc: SparkContext,
                                deps: Seq[Dependency[_]]) extends EdgeRDD[ED](sc, deps) {

  private[graphx] def grapePartitionsRDD: RDD[GrapeEdgePartition[VD, ED]] forSome { type VD }

  override def partitionsRDD = null

  def mapValues[ED2 : ClassTag](f: Edge[ED] => ED2): GrapeEdgeRDD[ED2]

  override def innerJoin[ED2: ClassTag, ED3: ClassTag](other: EdgeRDD[ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgeRDD[ED3]

  override def reverse: GrapeEdgeRDD[ED];
}

object GrapeEdgeRDD extends Logging{
  def fromPartitions[VD : ClassTag,ED : ClassTag](edgePartitions : RDD[GrapeEdgePartition[VD,ED]]) : GrapeEdgeRDDImpl[VD,ED] = {
    new GrapeEdgeRDDImpl[VD,ED](edgePartitions)
  }

  def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]): GrapeEdgeRDD[ED] = {
    //Shuffle the edge rdd
    //then use build to build.
    null
  }

  def fromEdgeShuffle[VD: ClassTag, ED : ClassTag](edgeShuffles : RDD[(PartitionID, EdgeShuffle[VD,ED])], defaultED : ED = null.asInstanceOf[ED]) : GrapeEdgeRDDImpl[VD,ED] = {
    //combine edges shuffles to edge Partition
    val numPartitions = edgeShuffles.getNumPartitions
    log.info(s"edgeShuffles has ${numPartitions} parts")

    val edgeShuffleReceived = edgeShuffles.mapPartitionsWithIndex((ind, iter) => {
      if (iter.hasNext) {
        val edgeShuffleReceived = new EdgeShuffleReceived[ED](ind)
        while (iter.hasNext) {
          val (pid, shuffle) = iter.next()
//          require(pid == ind)
          if (shuffle != null){
//              log.info(s"partition ${ind} receives msg from ${shuffle.fromPid}")
              edgeShuffleReceived.add(shuffle)
	        }
        }
//        log.info(s"Partition ${ind} collect received partitions ${edgeShuffleReceived}")
        Iterator((ind, edgeShuffleReceived))
      }
      else {
        Iterator.empty
      }
    },preservesPartitioning = true)
    fromEdgeShuffleReceived(edgeShuffleReceived,defaultED)
  }

  private [graphx] def fromEdgeShuffleReceived[VD: ClassTag, ED: ClassTag](edgesShuffles: RDD[(Int, EdgeShuffleReceived[ED])], defaultED : ED) : GrapeEdgeRDDImpl[VD,ED] = {
    val numPartitions = edgesShuffles.getNumPartitions
    //0. collection hostNames from edgeShuffles, store data in static class.
    //1. construct new rdd from hostNames and preferred locations.
    //2  get the stored shuffle data from static class.
    //3. construct rdd.

    val collectHosts = edgesShuffles.mapPartitionsWithIndex((pid, iter) =>{
      val hostName = InetAddress.getLocalHost.getHostName
      val (_pid,part) = iter.next()
      require(pid == _pid, s"not possible ${pid}, ${_pid}")
      EdgeShuffleReceived.push(part)
      Iterator(hostName)
    },preservesPartitioning = true).collect()

    log.info(s"shuffles exists on hosts ${collectHosts.mkString(",")}")

    val executorInfo = ExecutorInfoHelper.getExecutorsHost2Id(SparkContext.getOrCreate())
    val locations = collectHosts.map(host => {
      "executor_" + host + "_" + executorInfo.get(host).get
    })
    val sc = SparkContext.getOrCreate()
    log.info(s"hosts ${collectHosts.mkString(",")}, locations ${locations.mkString(",")}")
    val vineyardRDD = new VineyardRDD(sc, locations,collectHosts)

    val metaPartitions = vineyardRDD.mapPartitionsWithIndex((pid,iter) => {
      val client = iter.next()
      val grapeMeta = new GrapeMeta[VD,ED](pid, numPartitions, client, ExecutorUtils.getHostName)
      val edgePartitionBuilder = new GrapeEdgePartitionBuilder[VD,ED](numPartitions,client)
      edgePartitionBuilder.addEdges(EdgeShuffleReceived.get.asInstanceOf[EdgeShuffleReceived[ED]])
      val localVertexMap = edgePartitionBuilder.buildLocalVertexMap(pid)
      if (localVertexMap == null){
        Iterator.empty
      }
      else {
        grapeMeta.setLocalVertexMap(localVertexMap)
        grapeMeta.setEdgePartitionBuilder(edgePartitionBuilder)
        Iterator(grapeMeta)
      }
    },preservesPartitioning = true).cache()

    val localVertexMapIds = metaPartitions.mapPartitions(iter => {
      if (iter.hasNext){
        val meta = iter.next()
        Iterator(ExecutorUtils.getHostName + ":" + meta.partitionID + ":" + meta.localVertexMap.id())
      }
      else Iterator.empty
    },preservesPartitioning = true).collect().distinct.sorted

    log.info(s"[GrapeEdgeRDD]: got distinct local vm ids ${localVertexMapIds.mkString("Array(", ", ", ")")}")
//    require(localVertexMapIds.length == numPartitions, s"${localVertexMapIds.length} neq to num partitoins ${numPartitions}")

    log.info("[GrapeEdgeRDD]: Start constructing global vm")
    val time0 = System.nanoTime()
    val globalVMIDs = MPIUtils.constructGlobalVM(localVertexMapIds, ExecutorUtils.vineyardEndpoint, "int64_t", "uint64_t")
    log.info(s"[GrapeEdgeRDD]: Finish constructing global vm ${globalVMIDs}, cost ${(System.nanoTime() - time0)/1000000} ms")
    require(globalVMIDs.size() == numPartitions)

    val metaPartitionsUpdated = metaPartitions.mapPartitions(iter => {
      if (iter.hasNext) {
        val meta = iter.next()
        val hostName = InetAddress.getLocalHost.getHostName
        log.info(s"Doing meta update on ${}, pid ${meta.partitionID}")
        var res = null.asInstanceOf[String]
        var i = 0
        while (i < globalVMIDs.size()) {
          val ind = globalVMIDs.get(i).indexOf(hostName)
          if (ind != -1) {
            val spltted = globalVMIDs.get(i).split(":")
            require(spltted.length == 3) // hostname,pid,vmid
            require(spltted(0).equals(hostName))
            if (spltted(1).toInt == meta.partitionID) {
              res = spltted(2)
            }
          }
          i += 1
        }
        require(res != null, s"after iterate over received global ids, no suitable found for ${hostName}, ${meta.partitionID} : ${globalVMIDs}")
        meta.setGlobalVM(res.toLong)
        val (vm, csr) = meta.edgePartitionBuilder.buildCSR(meta.globalVMId)
        val edatas = meta.edgePartitionBuilder.buildEdataArray(csr, defaultED)
        //raw edatas contains all edge datas, i.e. csr edata array.
        //edatas are out edges edge cache.
        meta.setGlobalVM(vm)
        meta.setCSR(csr)
        meta.setEdataArray(edatas)
        meta.edgePartitionBuilder.clearBuilders()
        meta.edgePartitionBuilder = null //make it null to let it be gc able
        Iterator(meta)
      }
      else Iterator.empty
    },preservesPartitioning = true).cache()

    log.info(s"finish meta partitions updated ${metaPartitionsUpdated.count()}")


//    val grapeEdgePartitions = metaPartitionsUpdated.mapPartitions(iter => {
//      log.info("doing edge partition building")
//      if (iter.hasNext) {
//        val meta = iter.next()
//        val time0 = System.nanoTime()
//        val edgesNum = meta.graphxCSR.getOutEdgesNum.toInt
//        val srcOids = new Array[Long](edgesNum)
//        val srcLids = new Array[Long](edgesNum)
//        val dstOids = new Array[Long](edgesNum)
//        val dstLids = new Array[Long](edgesNum)
//        val eids = new Array[Long](edgesNum)
//        var curLid = 0
//        val endLid = meta.globalVM.innerVertexSize()
//        val vm = meta.globalVM
//        val oeOffsetsArray: ImmutableTypedArray[Long] = meta.graphxCSR.getOEOffsetsArray.asInstanceOf[ImmutableTypedArray[Long]]
//        log.info(s"all edges num: ${meta.graphxCSR.getOutEdgesNum}")
//        val lid2Oid : Array[Long] = {
//          val res = new Array[Long](vm.getVertexSize.toInt)
//          var i = 0;
//          val limit = vm.getVertexSize.toInt
//          while (i < limit){
//            res(i) = vm.getId(i)
//            i += 1
//          }
//          res
//        }
//        while (curLid < endLid){
//          val curOid = lid2Oid(curLid)
//          val startNbrOffset = oeOffsetsArray.get(curLid)
//          val endNbrOffset = oeOffsetsArray.get(curLid + 1)
//          var j = startNbrOffset.toInt
//          while (j < endNbrOffset){
//            srcOids(j) = curOid
//            srcLids(j) = curLid
//            j += 1
//          }
//          j = startNbrOffset.toInt
//          val nbr = meta.graphxCSR.getOEBegin(curLid)
//          while (j < endNbrOffset){
//            dstLids(j) = nbr.vid()
//            dstOids(j) = lid2Oid(nbr.vid().toInt)
//            eids(j) = nbr.eid()
//            nbr.addV(16);
//	          j += 1
//          }
//          curLid += 1
//        }
//        val time1 = System.nanoTime()
//        log.info(s"[Initializing edge cache in heap cost ]: ${(time1 - time0) / 1000000} ms")
//        val graphStructure = new GraphXGraphStructure(meta.globalVM,lid2Oid, meta.graphxCSR, srcLids, dstLids, srcOids, dstOids, eids)
//        Iterator(new GrapeEdgePartition[VD, ED](meta.partitionID, graphStructure, meta.vineyardClient, meta.edataArray))
//      }
//      else Iterator.empty
//    },preservesPartitioning = true).cache()
      val grapeEdgePartitions = metaPartitionsUpdated.mapPartitions(iter => {
        log.info("doing edge partition building")
        if (iter.hasNext) {
          val meta = iter.next()
          val time0 = System.nanoTime()
          val vm = meta.globalVM
          val lid2Oid : Array[Long] = {
            val res = new Array[Long](vm.getVertexSize.toInt)
            var i = 0;
            val limit = vm.getVertexSize.toInt
            while (i < limit){
              res(i) = vm.getId(i)
              i += 1
            }
            res
          }
          val time1 = System.nanoTime()
          val graphStructure = new GraphXGraphStructure(meta.globalVM,lid2Oid, meta.graphxCSR)
          log.info(s"[Creating graph structure cost ]: ${(time1 - time0) / 1000000} ms")
          Iterator(new GrapeEdgePartition[VD, ED](meta.partitionID, graphStructure, meta.vineyardClient, meta.edataArray))
        }
        else Iterator.empty
      },preservesPartitioning = true).cache()
    log.info(s"grape edge partition count ${grapeEdgePartitions.count()}")

    //clear cached builder memory
    metaPartitionsUpdated.unpersist()
    metaPartitions.unpersist()
    edgesShuffles.unpersist()

    val rdd = new GrapeEdgeRDDImpl[VD,ED](grapeEdgePartitions)
    log.info(s"[GrapeEdgeRDD:] Finish Construct EdgeRDD, total edges count ${rdd.count()}")
    rdd
  }
}
