package org.apache.spark.graphx.grape

import com.alibaba.graphscope.graphx.graph.impl.GraphXGraphStructure
import com.alibaba.graphscope.graphx.rdd.impl.{GrapeEdgePartition, GrapeEdgePartitionBuilder}
import com.alibaba.graphscope.graphx.rdd.{LocationAwareRDD, VineyardRDD}
import com.alibaba.graphscope.graphx.shuffle.{EdgeShuffle, EdgeShuffleReceived}
import com.alibaba.graphscope.graphx.store.InHeapVertexDataStore
import com.alibaba.graphscope.graphx.utils.{ArrayWithOffset, ExecutorUtils, GrapeMeta}
import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.graphx.grape.impl.GrapeEdgeRDDImpl
import org.apache.spark.graphx.scheduler.cluster.ExecutorInfoHelper
import org.apache.spark.graphx.{Edge, EdgeRDD, PartitionID, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import java.net.InetAddress
import scala.collection.mutable
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

  def fromEdgeShuffle[VD: ClassTag, ED : ClassTag](edgeShuffles : RDD[(PartitionID, EdgeShuffle[VD,ED])], defaultED : ED = null.asInstanceOf[ED], userNumPartitions : Int) : GrapeEdgeRDDImpl[VD,ED] = {
    //combine edges shuffles to edge Partition
    val edgeShuffleNumPartitions = edgeShuffles.getNumPartitions
    log.info(s"edgeShuffles has ${edgeShuffleNumPartitions} parts")

    val edgeShuffleReceived = edgeShuffles.mapPartitionsWithIndex((ind, iter) => {
      if (iter.hasNext) {
        val edgeShuffleReceived = new EdgeShuffleReceived[ED](ind)
        while (iter.hasNext) {
          val (pid, shuffle) = iter.next()
          if (shuffle != null){
              edgeShuffleReceived.add(shuffle)
	        }
        }
        Iterator((ind, edgeShuffleReceived))
      }
      else {
        Iterator.empty
      }
    },preservesPartitioning = true)
    fromEdgeShuffleReceived(edgeShuffleReceived,defaultED, userNumPartitions)
  }

  private [graphx] def fromEdgeShuffleReceived[VD: ClassTag, ED: ClassTag](edgesShuffles: RDD[(Int, EdgeShuffleReceived[ED])], defaultED : ED, userNumPartitions : Int) : GrapeEdgeRDDImpl[VD,ED] = {
    val numPartitions = edgesShuffles.getNumPartitions
    //0. collection hostNames from edgeShuffles, store data in static class.
    //1. construct new rdd from hostNames and preferred locations.
    //2  get the stored shuffle data from static class.
    //3. construct rdd.

    val collectHosts = edgesShuffles.mapPartitionsWithIndex((pid, iter) =>{
      if (iter.hasNext) {
        val hostName = InetAddress.getLocalHost.getHostName
        val (_pid, part) = iter.next()
        require(pid == _pid, s"not possible ${pid}, ${_pid}")
        EdgeShuffleReceived.push(part)
        Iterator(hostName)
      }
      else Iterator.empty
    },preservesPartitioning = true).collect()

    log.info(s"shuffles exists on hosts ${collectHosts.mkString(",")}")

    val executorInfo = ExecutorInfoHelper.getExecutorsHost2Id(SparkContext.getOrCreate())
    val locations = collectHosts.map(host => {
      "executor_" + host + "_" + executorInfo.get(host).get
    })
    val numExecutors = executorInfo.size
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
    require(localVertexMapIds.length == numPartitions, s"${localVertexMapIds.length} neq to num partitoins ${numPartitions}")

    log.info("[GrapeEdgeRDD]: Start constructing global vm")
    val time0 = System.nanoTime()
    val globalVMIDs = MPIUtils.constructGlobalVM(localVertexMapIds, ExecutorUtils.vineyardEndpoint, "int64_t", "uint64_t")
    log.info(s"[GrapeEdgeRDD]: Finish constructing global vm ${globalVMIDs}, cost ${(System.nanoTime() - time0)/1000000} ms")
    require(globalVMIDs.size() == numPartitions)
    log.info(s"meta partition size ${metaPartitions.count}")

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
        val (vm, csr) = meta.edgePartitionBuilder.buildCSR(meta.globalVMId,numExecutors)
        val eids = meta.edgePartitionBuilder.createEids(csr.getOutEdgesNum.toInt, csr.getOEBegin(0))
        val edatas = meta.edgePartitionBuilder.buildEdataArray(eids, defaultED)
        require(edatas.length == eids.length, s"neq ${edatas.length}, ${eids.length}")
        //raw edatas contains all edge datas, i.e. csr edata array.
        //edatas are out edges edge cache.
        meta.setGlobalVM(vm)
        meta.setEids(eids)
        meta.setCSR(csr)
        meta.setEdataArray(edatas)
        Iterator(meta)
      }
      else Iterator.empty
    },preservesPartitioning = true).cache()

    log.info(s"finish meta partitions updated ${metaPartitionsUpdated.count()}")

    metaPartitionsUpdated.foreachPartition(iter => {
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
        //set numSplit later
        val outerVertexDataStore = new InHeapVertexDataStore[VD](vm.innerVertexSize().toInt, vm.getOuterVertexSize.toInt, meta.vineyardClient,1, outer = true)
        val innerVertexDataStore = new InHeapVertexDataStore[VD](0, vm.innerVertexSize().toInt, meta.vineyardClient,0)
        //If vertex attr are in edge shuffles, we init the inner vertex Data store.
        val edgeBuilder = meta.edgePartitionBuilder
        val graphStructure = new GraphXGraphStructure(meta.globalVM,lid2Oid,meta.eids, meta.graphxCSR)
        edgeBuilder.fillVertexData(innerVertexDataStore,graphStructure)
        val time1 = System.nanoTime()
        log.info(s"[Creating graph structure cost ]: ${(time1 - time0) / 1000000} ms")
        GrapeEdgePartition.push((meta.partitionID,graphStructure, meta.vineyardClient, new ArrayWithOffset[ED](0,meta.edataArray), innerVertexDataStore,outerVertexDataStore))
        meta.edgePartitionBuilder.clearBuilders()
        meta.edgePartitionBuilder = null //make it null to let it be gc able
      }
    })

    //meta updated only has fnum partitions, we need to create userNumPartitions partitions.
    log.info(s"edge shuffle num partition ${numPartitions}, target num parts ${userNumPartitions}")
    val (expandedHosts, expandedLocations,expandedPartitionIds) = expand(globalVMIDs, executorInfo, userNumPartitions)
    require(expandedHosts.length == userNumPartitions)

    val emptyRDD = new LocationAwareRDD(sc, expandedLocations,expandedHosts,expandedPartitionIds)
    emptyRDD.foreachPartition(iter => {
      if (iter.hasNext){
        val part = iter.next()
        lazy val hostName = InetAddress.getLocalHost.getHostName
        require(hostName.equals(part.hostName), s"part ${part.ind} host name neq ${part.hostName}, ${InetAddress.getLocalHost.getHostName}")
        GrapeEdgePartition.incCount(part.ind)
      }
    })
    emptyRDD.foreachPartition(iter => {
      if (iter.hasNext){
        GrapeEdgePartition.createPartitions[VD,ED](iter.next().ind,userNumPartitions)
      }
    })
    log.info(s"empty rdd size ${emptyRDD.getNumPartitions}")

    val grapeEdgePartitions = emptyRDD.mapPartitionsWithIndex((pid, iter) => {
      if (iter.hasNext){
        val grapeEdgePartition = GrapeEdgePartition.get[VD,ED](pid)
        log.info(s"part ${pid} got grapeEdgePart ${grapeEdgePartition.toString}")
//        val fid = grapeEdgePartition.graphStructure.fid()
//        val ppid = grapeEdgePartition.graphStructure.fid2GraphxPid(fid)
//        require(pid == ppid, s"partition id ${pid} neq ${ppid}")
        Iterator(grapeEdgePartition)
      }
      else Iterator.empty
    },preservesPartitioning = true).cache()

    //clear cached builder memory
    metaPartitionsUpdated.unpersist()
    metaPartitions.unpersist()

    val rdd = new GrapeEdgeRDDImpl[VD,ED](grapeEdgePartitions)
    log.info(s"[GrapeEdgeRDD:] Finish Construct EdgeRDD, total edges count ${rdd.count()}")
    rdd
  }

  def expand(globalVMIds: java.util.List[String], executorInfo: mutable.HashMap[String,String], targetLength : Int) : (Array[String],Array[String],Array[Int]) = {
    val size = globalVMIds.size()
    val hostNames = new Array[String](targetLength)
    val locations = new Array[String](targetLength)
    val partitionIds = new Array[Int](targetLength)
    for (i <- 0 until size){
      val splited = globalVMIds.get(i).split(":")
      require(splited.length == 3)
      hostNames(i) = splited(0)
      locations(i) = "executor_" + hostNames(i) + "_" + executorInfo(hostNames(i))
      partitionIds(i) = splited(1).toInt
    }

    var i = size
    while (i < targetLength){
      hostNames(i) = hostNames(i % size)
      locations(i) = locations(i % size)
      partitionIds(i) = i
      i += 1
    }
    log.info(s"expanded partitions,host names ${hostNames.mkString("Array(", ", ", ")")}")
    log.info(s"expanded partitions,locations ${locations.mkString("Array(", ", ", ")")}")
    log.info(s"expanded partitions,partitionIds ${partitionIds.mkString("Array(", ", ", ")")}")
    (hostNames, locations, partitionIds)
  }
}
