package org.apache.spark.graphx.grape

import com.alibaba.graphscope.ds.{ImmutableTypedArray, Vertex}
import com.alibaba.graphscope.graphx.graph.impl.GraphXGraphStructure
import com.alibaba.graphscope.graphx.rdd.VineyardRDD
import com.alibaba.graphscope.graphx.rdd.impl.{GrapeEdgePartition, GrapeEdgePartitionBuilder}
import com.alibaba.graphscope.graphx.shuffle.{EdgeShuffle, EdgeShuffleReceived}
import com.alibaba.graphscope.graphx.utils.{ExecutorUtils, GrapeMeta}
import com.alibaba.graphscope.utils.array.PrimitiveArray
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

  def fromEdgeShuffle[VD: ClassTag, ED : ClassTag](edgeShuffles : RDD[(PartitionID, EdgeShuffle[VD,ED])]) : GrapeEdgeRDDImpl[VD,ED] = {
    //combine edges shuffles to edge Partition
    val numPartitions = edgeShuffles.getNumPartitions
    log.info(s"edgeShuffles has ${numPartitions} parts")

    val edgeShuffleReceived = edgeShuffles.mapPartitionsWithIndex((ind, iter) => {
      if (iter.hasNext) {
        val edgeShuffleReceived = new EdgeShuffleReceived[ED](ind)
        while (iter.hasNext) {
          val (pid, shuffle) = iter.next()
          require(pid == ind)
          if (shuffle != null){
              log.info(s"partition ${ind} receives msg from ${shuffle.fromPid}")
              edgeShuffleReceived.add(shuffle)
	        }
        }
        log.info(s"Partition ${ind} collect received partitions ${edgeShuffleReceived}")
        Iterator((ind, edgeShuffleReceived))
      }
      else {
        Iterator.empty
      }
    },preservesPartitioning = true)
    fromEdgeShuffleReceived(edgeShuffleReceived)
  }

  private [graphx] def fromEdgeShuffleReceived[VD: ClassTag, ED: ClassTag](edgesShuffles: RDD[(Int, EdgeShuffleReceived[ED])]) : GrapeEdgeRDDImpl[VD,ED] = {
    val numPartitions = edgesShuffles.getNumPartitions
    //0. collection hostNames from edgeShuffles, store data in static class.
    //1. construct new rdd from hostNames and preferred locations.
    //2  get the stored shuffle data from static class.
    //3. construct rdd.

    val collectHosts = edgesShuffles.mapPartitionsWithIndex((pid, iter) =>{
      val hostName = InetAddress.getLocalHost.getHostName
      val (_pid,part) = iter.next()
      require(pid == _pid, s"not possible ${pid}, ${_pid}")
      EdgeShuffleReceived.set(part)
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
    //test correctness
    val tmp1 = sc.parallelize(Array(0,1), numPartitions)
    val vineyard2 = vineyardRDD.zipPartitions(tmp1,preservesPartitioning = true){
      (iter1, iter2) => {
        val client = iter1.next()
        val value = iter2.next()
        log.info(s"got value first ${value}")
        Iterator(client)
      }
    }
    log.info(s"after zip on vineyard rdd ${vineyard2.count()}")
    val vineyard3 = vineyardRDD.zipPartitions(vineyard2,preservesPartitioning = true){
      (iter1, iter2) => {
        val client = iter1.next()
        val value = iter2.next()
        log.info(s"got value second ${value}")
        Iterator(client)
      }
    }
    log.info(s"after zip on vineyard rdd ${vineyard3.count()}")

    val metaPartitions = vineyard3.mapPartitionsWithIndex((pid,iter) => {
      val client = iter.next()
      log.info(s"client ${client}")
      val grapeMeta = new GrapeMeta[VD,ED](pid, numPartitions, client, ExecutorUtils.getHostName)
      val edgePartitionBuilder = new GrapeEdgePartitionBuilder[VD,ED](numPartitions,client)
      require(EdgeShuffleReceived.data != null, s"edge shuffles should be set, but not found on ${InetAddress.getLocalHost.getHostName}, ${grapeMeta.partitionID}")
      edgePartitionBuilder.addEdges(EdgeShuffleReceived.data.asInstanceOf[EdgeShuffleReceived[ED]])
      val localVertexMap = edgePartitionBuilder.buildLocalVertexMap()
      grapeMeta.setLocalVertexMap(localVertexMap)
      grapeMeta.setEdgePartitionBuilder(edgePartitionBuilder)
      Iterator(grapeMeta)
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
    val globalVMIDs = MPIUtils.constructGlobalVM(localVertexMapIds, ExecutorUtils.vineyardEndpoint, "int64_t", "uint64_t")
    log.info(s"[GrapeEdgeRDD]: Finish constructing global vm ${globalVMIDs}")
    require(globalVMIDs.size() == numPartitions)

    val metaPartitionsUpdated = metaPartitions.mapPartitions(iter => {
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
      val edatas = meta.edgePartitionBuilder.buildEdataArray(csr)
      //raw edatas contains all edge datas, i.e. csr edata array.
      //edatas are out edges edge cache.
      meta.setGlobalVM(vm)
      meta.setCSR(csr)
      meta.setEdataArray(edatas)
      meta.edgePartitionBuilder.clearBuilders()
      meta.edgePartitionBuilder = null //make it null to let it be gc able
      Iterator(meta)
    },preservesPartitioning = true).cache()

    log.info(s"finish meta partitions updated ${metaPartitionsUpdated.count()}")


    val grapeEdgePartitions = metaPartitionsUpdated.mapPartitions(iter => {
      log.info("doing edge partition building")
      if (iter.hasNext) {
        val meta = iter.next()
        val time0 = System.nanoTime()
        val edgesNum = meta.graphxCSR.getOutEdgesNum.toInt
        val srcOids = PrimitiveArray.create(classOf[Long], edgesNum)
        val srcLids = PrimitiveArray.create(classOf[Long], edgesNum)
        val dstOids = PrimitiveArray.create(classOf[Long], edgesNum)
        val dstLids = PrimitiveArray.create(classOf[Long], edgesNum)
        val eids = PrimitiveArray.create(classOf[Long], edgesNum)
        val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
        var curLid = 0
        val endLid = meta.globalVM.innerVertexSize()
        val vm = meta.globalVM
        val oeOffsetsArray: ImmutableTypedArray[Long] = meta.graphxCSR.getOEOffsetsArray.asInstanceOf[ImmutableTypedArray[Long]]
        log.info(s"all edges num: ${meta.graphxCSR.getOutEdgesNum}")
        while (curLid < endLid){
          val curOid = meta.globalVM.getId(curLid)
          val startNbrOffset = oeOffsetsArray.get(curLid)
          val endNbrOffset = oeOffsetsArray.get(curLid + 1)
//          log.info(s" begin offset ${startNbrOffset}, end offset ${endNbrOffset}, out degree for ${curOid} ${meta.graphxCSR.getOutDegree(curLid)}")
          var j = startNbrOffset
          while (j < endNbrOffset){
            srcOids.set(j, curOid)
            srcLids.set(j, curLid)
            j += 1
          }
          j = startNbrOffset
          val nbr = meta.graphxCSR.getOEBegin(curLid)
          while (j < endNbrOffset){
            dstLids.set(j, nbr.vid())
            vertex.SetValue(nbr.vid())
            dstOids.set(j, vm.getId(nbr.vid()))
            eids.set(j, nbr.eid())
//            log.info(s"visiting edge ${curLid}->${nbr.vid()}, eid ${nbr.eid()}")
            nbr.addV(16);
	          j += 1
          }
          curLid += 1
        }
        val time1 = System.nanoTime()
        log.info(s"[Initializing edge cache in heap cost ]: ${(time1 - time0) / 1000000} ms")
        val graphStructure = new GraphXGraphStructure(meta.globalVM, meta.graphxCSR, srcLids, dstLids, srcOids, dstOids, eids)
        Iterator(new GrapeEdgePartition[VD, ED](meta.partitionID, graphStructure, meta.vineyardClient, meta.edataArray))
      }
      else Iterator.empty
    },preservesPartitioning = true).cache()
    log.info(s"grape edge partition count ${grapeEdgePartitions.count()}")

    //clear cached builder memory
    metaPartitionsUpdated.unpersist()

    val rdd =new GrapeEdgeRDDImpl[VD,ED](grapeEdgePartitions)
    log.info(s"[GrapeEdgeRDD:] Finish Construct EdgeRDD, total edges count ${rdd.count()}")
    rdd
  }
}
