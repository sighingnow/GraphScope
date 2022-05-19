package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.ds.ImmutableTypedArray
import com.alibaba.graphscope.graphx.{GraphXCSR, GraphXVertexMap, LocalVertexMap, VineyardClient}
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.impl.{EdgePartition, GrapeUtils}
import org.apache.spark.graphx.utils.{ExecutorUtils, ScalaFFIFactory}
import org.apache.spark.graphx.{Edge, ReusableEdge, ReusableEdgeImpl, ReversedReusableEdge}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * [startLid, endLid), endLid is exclusive
 */
class GrapeEdgePartition[VD: ClassTag, ED: ClassTag](val pid : Int,
                                                     val csr : GraphXCSR[Long,ED],
                                                     val vm : GraphXVertexMap[Long,Long],
                                                     val client : VineyardClient,
                                                     val edgeReversed : Boolean = false,
                                                     var activeSet : BitSet = null) extends Logging {
  val startLid = 0
  val endLid : Long = vm.innerVertexSize()
  def partOutEdgeNum : Long = csr.getPartialOutEdgesNum(startLid, endLid)
  def partInEdgeNum : Long = csr.getPartialInEdgesNum(startLid, endLid)
  if (activeSet == null){
    activeSet = new BitSet(csr.getOEOffset(endLid).toInt) // just need to control out edges
  }
  val treeMap = new java.util.TreeMap[Long,Long]()
  initTreeMap
  val NBR_SIZE = 16L
  val edataArray : ImmutableTypedArray[ED] = csr.getEdataArray

  log.info(s"Got edge partition ${this.toString}")

  /**
   * Get the attr of edges.
   * @param eid edge id, not offset.
   * @param offset offset, indicate the csr offset.
   * @return
   */
  def getEdata(eid : Long) : ED = {
    edataArray.get(eid)
  }

  def initTreeMap : Unit = {
    var curLid = startLid
    val time0 = System.nanoTime()
    while (curLid < endLid){
      val ind = csr.getOEOffset(curLid)
      treeMap.put(ind, curLid)
      log.info(s"Init treemap ${ind}: ${curLid}")
      curLid += 1
    }
    val time1 = System.nanoTime()
    log.info(s"Init treemap cost ${(time1 - time0) / 1000000} ms")
  }

  /** Iterate over out edges only, in edges will be iterated in other partition */
  def iterator : Iterator[Edge[ED]] = {
    new Iterator[Edge[ED]]{
      val beginAddress = csr.getOEBegin(0).getAddress
      val curNbr = csr.getOEBegin(0)
      var index = activeSet.nextSetBit(csr.getOEOffset(startLid).toInt)
      var edge = null.asInstanceOf[ReusableEdge[ED]]

      if (edgeReversed){
        edge = new ReversedReusableEdge[ED];
      }
      else {
        edge = new ReusableEdgeImpl[ED];
      }
      log.info(s"Initiate iterator on partition ${pid} ,reversed ${edgeReversed}")

      override def hasNext: Boolean = {
        index >= 0
      }

      override def next(): Edge[ED] = {
        val curAddress = beginAddress + index * NBR_SIZE
        curNbr.setAddress(curAddress)
        log.info(s"[in next], index ${}, curAddress ${curAddress}")
        val dstLid = curNbr.vid()
        val edata = getEdata(curNbr.eid())
        val srcLid = treeMap.floorKey(index)
        log.info(s"[In next], srcLid ${srcLid}, dstLid ${dstLid}, edata ${edata}")
        edge.srcId = vm.getId(srcLid)
        edge.dstId = vm.getId(dstLid)
        edge.attr = edata
        log.info(s"[In next], srcOid ${edge.srcId}, dstOid ${edge.dstId}, edata ${edata}")
        index = activeSet.nextSetBit(index + 1)
        edge
      }
    }
  }


  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[VD, ED2] = {
    val newData = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED2], csr.getEdataArray.getLength.toInt).asInstanceOf[PrimitiveArray[ED2]]
    val iter = iterator
    var ind = 0;
    while (iter.hasNext){
      newData.set(ind, f(iter.next()))
      ind += 1
    }
    this.withNewEdata(newData)
  }

  def reverse: GrapeEdgePartition[VD, ED] = {
    new GrapeEdgePartition[VD,ED](pid, csr, vm, client,!edgeReversed, activeSet)
  }

  def withNewEdata[ED2: ClassTag](newEdata : PrimitiveArray[ED2]): GrapeEdgePartition[VD, ED2] = {
    //FIXME: decouple ed from csr.
    new GrapeEdgePartitionWithDataCache[VD,ED2](pid, csr.asInstanceOf[GraphXCSR[Long,ED2]],vm, client,edgeReversed,activeSet,newEdata)
  }

  override def toString: String = "GrapeEdgePartition(pid=" + pid +
    ", start lid" + startLid + ", end lid " + endLid + ",csr: " + csr + ", vm" + vm.toString +
    ",out edges num" + partOutEdgeNum + ", in edges num" + partInEdgeNum +")"
}

class GrapeEdgePartitionBuilder[VD: ClassTag, ED: ClassTag](val client : VineyardClient) extends Logging{
  val srcOidBuilder: ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
  val dstOidBuilder: ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
  val edataBuilder : ArrowArrayBuilder[ED] = ScalaFFIFactory.newArrowArrayBuilder(GrapeUtils.getRuntimeClass[ED].asInstanceOf[Class[ED]])
  val innerOidBuilder : ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
  val outerOidBuilder : ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
  val lists : ArrayBuffer[EdgeShuffleReceived[ED]] = ArrayBuffer.empty[EdgeShuffleReceived[ED]]
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
        log.info(s"edge shuffle ${edgeShuffle} size ${edgeShuffle.size()}")
        val receivedOids = edgeShuffle.oids
        log.info(s"Before union with ${receivedOids.size}, size ${innerHashSet.size}")
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
    val localVertexMapBuilder = ScalaFFIFactory.newLocalVertexMapBuilder(client, innerOidBuilder, outerOidBuilder)
    val localVM = localVertexMapBuilder.seal(client).get();
    log.info(s"${ExecutorUtils.getHostName}: Finish building local vm: ${localVM.id()}, ${localVM.getInnerVerticesNum}");
    localVM
  }

  def buildCSR(globalVMID : Long): (GraphXVertexMap[Long,Long], GraphXCSR[Long,ED]) = {
    val edgesNum = lists.map(shuffle => shuffle.totalSize()).sum
    log.info(s"Got totally ${lists.length}, edges ${edgesNum} in ${ExecutorUtils.getHostName}")
    srcOidBuilder.reserve(edgesNum)
    dstOidBuilder.reserve(edgesNum)
    edataBuilder.reserve(edgesNum)
    log.info(s"Constructing csr with global vm ${globalVMID}")
    val graphxVertexMapGetter = ScalaFFIFactory.newVertexMapGetter()
    val graphxVertexMap = graphxVertexMapGetter.get(client, globalVMID).get()
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
    val graphxCSRBuilder = ScalaFFIFactory.newGraphXCSRBuilder[ED](client)
    graphxCSRBuilder.loadEdges(srcOidBuilder,dstOidBuilder,edataBuilder,graphxVertexMap)
    val graphxCSR = graphxCSRBuilder.seal(client).get()
    (graphxVertexMap,graphxCSR)
  }
}
