package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.graphx.{GraphXCSR, GraphXVertexMap, LocalVertexMap, VineyardClient}
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.utils.{ExecutorUtils, ScalaFFIFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class GrapeEdgePartition[VD: ClassTag, ED: ClassTag](val pid : Int, val startLid : Long, val endLid : Long,
                                                     val csr : GraphXCSR[Long,ED],
                                                     val vm : GraphXVertexMap[Long,Long]) {
  def partEdgeNum : Long = csr.getPartialOutEdgesNum(startLid, endLid)

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
  val outerOidBuilder : ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
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
  def buildLocalVertexMap() : LocalVertexMap[Long,Long] = {
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
    //Build outer oids
    val outerHashSet = new OpenHashSet[Long]
    for (shuffle <- lists){
      log.info(s"Extract outer vertices from ${shuffle}")
      val (srcArrays, dstArrays, attrArrays) = shuffle.getArrays
      var i = 0
      val outerArrayLimit = srcArrays.length
      while (i < outerArrayLimit){
        var j = 0
        val innerLimit = srcArrays(i).length
        require(dstArrays(i).length == innerLimit)
        require(attrArrays(i).length == innerLimit)
        val srcArray = srcArrays(i)
        val dstArray = dstArrays(i)
        while (j < innerLimit){
          if (!innerHashSet.contains(srcArray(j))){
            outerHashSet.add(srcArray(j))
          }
          if (!innerHashSet.contains(dstArray(j))){
            outerHashSet.add(dstArray(j))
          }
          j += 1
        }
        i += 1
      }
    }
    outerOidBuilder.reserve(outerHashSet.size)
    val outerIter = outerHashSet.iterator
    while (outerIter.hasNext){
      outerOidBuilder.unsafeAppend(outerIter.next())
    }
    val localVertexMapBuilder = ScalaFFIFactory.newLocalVertexMapBuilder(ExecutorUtils.getVineyarClient, innerOidBuilder, outerOidBuilder)
    val localVM = localVertexMapBuilder.seal(ExecutorUtils.getVineyarClient).get();
    log.info(s"${ExecutorUtils.getHostName}: Finish building local vm: ${localVM.id()}, ${localVM.getInnerVerticesNum}");
    localVMBuilt = true
    localVM
  }

  def buildCSR(): GraphXCSR[Long,_] = {
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
      val (srcArrays, dstArrays, attrArrays) = shuffle.getArrays
      var i = 0
      val outerArrayLimit = srcArrays.length
      while (i < outerArrayLimit){
        var j = 0
        val innerLimit = srcArrays(i).length
        require(dstArrays(i).length == innerLimit)
        require(attrArrays(i).length == innerLimit)
        val srcArray = srcArrays(i)
        val dstArray = dstArrays(i)
        val attrArray = attrArrays(i)
        while (j < innerLimit){
          srcOidBuilder.unsafeAppend(srcArray(j))
          dstOidBuilder.unsafeAppend(dstArray(j))
          edataBuilder.unsafeAppend(attrArray(j))
          j += 1
        }
        i += 1
      }
//      val iter = shuffle.iterator()
//      while (iter.hasNext){
//        val edge = iter.next()
////        log.info(s"processing edge ${edge.srcId}->${edge.dstId}, ${edge.attr}")
//        srcOidBuilder.unsafeAppend(edge.srcId)
//        dstOidBuilder.unsafeAppend(edge.dstId)
//        edataBuilder.unsafeAppend(edge.attr)
//      }
    }
    log.info("Finish adding edges to builders")
    val graphxCSRBuilder = ScalaFFIFactory.newGraphXCSRBuilder[ED](ExecutorUtils.getVineyarClient)
    graphxCSRBuilder.loadEdges(srcOidBuilder,dstOidBuilder,edataBuilder,graphxVertexMap)
    val graphxCSR = graphxCSRBuilder.seal(ExecutorUtils.getVineyarClient).get()
    ExecutorUtils.setGraphXCSR(graphxCSR)
    ExecutorUtils.setGlobalVM(graphxVertexMap)
    csrBuilt = true
    graphxCSR
  }

  def getEdgePartition(pid: Int): GrapeEdgePartition[VD,ED] ={
    val localPartNum = ExecutorUtils.getPartitionNum
    val grapePartId = ExecutorUtils.graphXPid2GrapePid(pid)
    log.info(s"got partition ${pid}'s corresponding grape partition ${grapePartId}")
    val graphXCSR = ExecutorUtils.getGraphXCSR.asInstanceOf[GraphXCSR[Long,ED]]
    val vertexMap = ExecutorUtils.getGlobalVM
    require(vertexMap.innerVertexSize.equals(graphXCSR.vertexNum()), s"csr inner vertex should equal to vmap ${vertexMap.innerVertexSize()}, ${graphXCSR.vertexNum()}")
    val ivnum = vertexMap.innerVertexSize()
    val chunkSize = (ivnum + localPartNum - 1) / localPartNum
    val begin = chunkSize * grapePartId
    val end = Math.min(begin + chunkSize, ivnum)
    log.info(s"Part ${pid}, grape Pid ${grapePartId} got range (${begin}, ${end})")
    new GrapeEdgePartition[VD,ED](pid, begin, end, graphXCSR, vertexMap)
  }

  def isLocalBuilt() = localVMBuilt

  def isGlobalBuilt() = globalVMBuilt

  def isCSRBuilt() = csrBuilt
}
