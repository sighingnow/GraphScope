package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.utils.{ExecutorUtils, ScalaFFIFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class GrapeEdgePartition[VD: ClassTag, ED: ClassTag](val pid : Int, val localNum : Int) {
  def partEdgeNum : Long = 0

  def iterator : Iterator[Edge[ED]] = {
    null
  }
  def reverse: GrapeEdgePartition[VD, ED] = {
    null
  }
  def map[ED2: ClassTag](iter: Iterator[ED2]): GrapeEdgePartition[VD, ED2] = {
    null
  }
  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[VD, ED2] = {
    null
  }
}

class GrapeEdgePartitionBuilder[VD: ClassTag, ED: ClassTag](val client : VineyardClient) extends Logging{
  val srcOidBuilder: ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
  val dstOidBuilder: ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
  val edataBuilder : ArrowArrayBuilder[ED] = ScalaFFIFactory.newArrowArrayBuilder(GrapeUtils.getRuntimeClass[ED].asInstanceOf[Class[ED]])
  val innerOidBuilder : ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
  val lists : ArrayBuffer[EdgeShuffleReceived[ED]] = ArrayBuffer.empty[EdgeShuffleReceived[ED]]
  var localVMBuilt = false
  var globalVMBuilt = false
  var csrBuilt = false
  //Concurrency control should be done by upper level
  def addEdges(edges : EdgeShuffleReceived[ED]) : Unit = {
    lists.+=(edges)
  }

  /**
   * @return the built local vertex map id.
   */
  def buildLocalVertexMap() : Long = {
    //We need to get oid->lid mappings in this executor.
    val innerHashSet = new OpenHashSet[Long]
    for (edgeShuffleReceive <- lists){
      for (edgeShuffle <- edgeShuffleReceive.fromPid2Shuffle){
        val receivedOids = edgeShuffle.oids
        log.info(s"Before union with ${receivedOids}, size ${innerHashSet.size}")
        innerHashSet.union(receivedOids)
        log.info(s"after ${innerHashSet.size}")
      }
    }
    log.info(s"Found totally ${innerHashSet.size} in ${ExecutorUtils.getHostName}")
    innerOidBuilder.reserve(innerHashSet.size)
    val iter = innerHashSet.iterator
    while (iter.hasNext){
      innerOidBuilder.unsafeAppend(iter.next());
    }
    val localVertexMapBuilder = ScalaFFIFactory.newLocalVertexMapBuilder(ExecutorUtils.getVineyarClient, innerOidBuilder)
    val localVM = localVertexMapBuilder.seal(ExecutorUtils.getVineyarClient).get();
    log.info(s"${ExecutorUtils.getHostName}: Finish building local vm: ${localVM.id()}, ${localVM.getInnerVerticesNum}");
    localVMBuilt = true
    localVM.id()
  }

  def buildCSR(): Long = {
    val edgesNum = lists.map(shuffle => shuffle.totalSize()).sum
    log.info(s"Got totally ${lists.length}, edges ${edgesNum} in ${ExecutorUtils.getHostName}")
    srcOidBuilder.reserve(edgesNum)
    dstOidBuilder.reserve(edgesNum)
    edataBuilder.reserve(edgesNum)
    val globalVMID = ExecutorUtils.getGlobalVMID
    log.info(s"Constructing csr with global vm ${globalVMID}")
    val graphxVertexMapGetter = ScalaFFIFactory.newVertexMapGetter()
    val graphxVertexMap = graphxVertexMapGetter.get(ExecutorUtils.getVineyarClient, globalVMID).get()
    log.info(s"Got graphx vertex map: ${graphxVertexMap}, total vnum ${graphxVertexMap.getTotalVertexSize}, fid ${graphxVertexMap.fid()}/${graphxVertexMap.fnum()}")
    for (shuffle <- lists){
      log.info(s"Processing ${shuffle}")
      val iter = shuffle.iterator()
      while (iter.hasNext){
        val edge = iter.next()
        log.info(s"processing edge ${edge.srcId}->${edge.dstId}, ${edge.attr}")
        srcOidBuilder.unsafeAppend(edge.srcId)
        dstOidBuilder.unsafeAppend(edge.dstId)
        edataBuilder.unsafeAppend(edge.attr)
      }
    }
    log.info("Finish adding edges to builders")
    val graphxCSRBuilder = ScalaFFIFactory.newGraphXCSRBuilder[ED](ExecutorUtils.getVineyarClient)
    graphxCSRBuilder.loadEdges(srcOidBuilder,dstOidBuilder,edataBuilder,graphxVertexMap)
    val graphxCSR = graphxCSRBuilder.seal(ExecutorUtils.getVineyarClient).get()
    ExecutorUtils.setGraphXCSR(graphxCSR)
    ExecutorUtils.setGlobalVM(graphxVertexMap)
    csrBuilt = true
    graphxCSR.id()
  }

  def isLocalBuilt() = localVMBuilt

  def isGlobalBuilt() = globalVMBuilt

  def isCSRBuilt() = csrBuilt

  def getEdgePartition(pid: Int): GrapeEdgePartition[VD,ED] ={
    var localPartNum = ExecutorUtils.getPartitionNum
    log.info(s"got partition ${pid}'s corresponding grape partition'")
    null
  }
}
