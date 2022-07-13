package com.alibaba.graphscope.graphx.rdd.impl

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.ds.PropertyNbrUnit
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.graph.{GSEdgeTriplet, GraphStructure, ReusableEdge}
import com.alibaba.graphscope.graphx.shuffle.EdgeShuffleReceived
import com.alibaba.graphscope.graphx.store.{InHeapVertexDataStore, VertexDataStore}
import com.alibaba.graphscope.graphx.utils.{ArrayWithOffset, BitSetWithOffset, ExecutorUtils, IdParser, ScalaFFIFactory}
import org.apache.spark.Partition
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * [startLid, endLid), endLid is exclusive
 * FIXME: should not be serializable
 */
class GrapeEdgePartition[VD: ClassTag, ED: ClassTag](val pid : Int,
                                                     val localId : Int, // the order of partition in this fragment.
                                                     val startLid : Long,
                                                     val endLid : Long,
                                                     val graphStructure: GraphStructure,
                                                     val client : VineyardClient,
                                                     var edatas : ArrayWithOffset[ED],
                                                     //the original edatas are fully-specified,i.e. length equal to oeEdgeNum. but later we will use array with offset.
                                                     val edgeReversed : Boolean = false,
                                                     var activeEdgeSet : BitSetWithOffset = null) extends Logging  with Partition{
//  val startLid = 0
//  val endLid : Long = graphStructure.getInnerVertexSize
  val getVertexSize = endLid - startLid

  /** regard to our design, the card(activeEdgeSet) == partOutEdgeNum, len(edatas) == allEdgesNum, indexed by eid */
  val partOutEdgeNum : Long = graphStructure.getOutEdgesNum
  val partInEdgeNum : Long = graphStructure.getInEdgesNum
  val allEdgesNum = partInEdgeNum + partOutEdgeNum


  if (activeEdgeSet == null){
    val (startOffset,endOffset) = graphStructure.getOEOffsetRange(startLid,endLid)
    activeEdgeSet = new BitSetWithOffset(startBit = startOffset.toInt,endBit = endOffset.toInt)
    activeEdgeSet.setRange(startOffset, endOffset)
  }
  val NBR_SIZE = 16L
  //to avoid the difficult to get srcLid in iterating over edges.

  log.info(s"Got edge partition ${this.toString}")


  def getDegreeArray(edgeDirection: EdgeDirection): Array[Int] = {
    if (edgeDirection.equals(EdgeDirection.In)){
      graphStructure.inDegreeArray(startLid,endLid)
    }
    else if (edgeDirection.equals(EdgeDirection.Out)){
      graphStructure.outDegreeArray(startLid,endLid)
    }
    else{
      graphStructure.inOutDegreeArray(startLid,endLid)
    }
  }

  /** Iterate over out edges only, in edges will be iterated in other partition */
  def iterator : Iterator[Edge[ED]] = {
    graphStructure.iterator(startLid, endLid,edatas,activeEdgeSet,edgeReversed)
  }

  def tripletIterator(innerVertexDataStore: VertexDataStore[VD],outerVertexDataStore: VertexDataStore[VD],
                       includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false, includeLid : Boolean = false)
  : Iterator[EdgeTriplet[VD, ED]] = graphStructure.tripletIterator(startLid,endLid,innerVertexDataStore,outerVertexDataStore,edatas,activeEdgeSet,edgeReversed,includeSrc,includeDst, reuseTriplet, includeLid)

  def filter(
              epred: EdgeTriplet[VD, ED] => Boolean,
              vpred: (VertexId, VD) => Boolean,
              innerVertexDataStore: VertexDataStore[VD],outerVertexDataStore : VertexDataStore[VD]): GrapeEdgePartition[VD, ED] = {
    //First invalided all invalid edges from invalid vertices.
    val tripletIter = tripletIterator(innerVertexDataStore,outerVertexDataStore, true, true, true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    val newActiveEdges = new BitSetWithOffset(activeEdgeSet.startBit,activeEdgeSet.endBit)
    newActiveEdges.union(activeEdgeSet)
    while (tripletIter.hasNext){
      val triplet = tripletIter.next()
      if (!vpred(triplet.srcId,triplet.srcAttr) || !vpred(triplet.dstId, triplet.dstAttr) || !epred(triplet)){
        newActiveEdges.unset(triplet.offset.toInt)
//        log.info(s"Inactive edge ${triplet}")
      }
    }
    this.withNewMask(newActiveEdges)
  }

  def groupEdges(merge: (ED, ED) => ED): GrapeEdgePartition[VD, ED] = {
    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    var curSrcId = -1L
    var curDstId = -1L
    var prevEdgeInd = -1L // when we find a new one, we unset prevEdgeInd
    var attrSum = null.asInstanceOf[ED]
    val newMask = new BitSetWithOffset(activeEdgeSet.startBit,activeEdgeSet.endBit)
    newMask.union(activeEdgeSet)
    val newEdata = new ArrayWithOffset[ED](activeEdgeSet.startBit,activeEdgeSet.size)// new Array[ED](allEdgesNum.toInt)
    while (iter.hasNext){
      val edge = iter.next()
      val curIndex = edge.offset
      var flag = false
      if (flag && edge.srcId == curSrcId && edge.dstId == curDstId){
        attrSum = merge(attrSum, edge.attr)
        newMask.unset(prevEdgeInd.toInt)
        prevEdgeInd = curIndex
        log.info(s"Merge edge (${curSrcId}, ${curDstId}), val ${attrSum}")
      }
      else {
        if (flag){
          //a new round start, we add new Edata to this edge
          newEdata(prevEdgeInd.toInt) = attrSum
          log.info(s"end of acculating edge ${curSrcId}, ${curDstId}, ${attrSum}")
        }
        curSrcId = edge.srcId
        curDstId = edge.dstId
        prevEdgeInd = curIndex
        attrSum = edge.attr
      }
      flag = true
    }
    new GrapeEdgePartition[VD,ED](pid,localId, startLid, endLid, graphStructure, client,newEdata, edgeReversed, newMask)
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[VD, ED2] = {
    val newData = new ArrayWithOffset[ED2](activeEdgeSet.startBit, activeEdgeSet.size)// new Array[ED2](allEdgesNum.toInt)
    graphStructure.iterateEdges(startLid, endLid,f,edatas, activeEdgeSet, edgeReversed, newData)
    this.withNewEdata(newData)
  }

  def map[ED2: ClassTag](f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): GrapeEdgePartition[VD, ED2] = {
    val time0 = System.nanoTime()
    val newData = new ArrayWithOffset[ED2](activeEdgeSet.startBit, activeEdgeSet.size)
    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    val resultEdata = f(pid, iter)
    val time1 = System.nanoTime()
    var ind = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
    while (ind >= 0 && resultEdata.hasNext){
      newData(ind) = resultEdata.next()
      ind = activeEdgeSet.nextSetBit(ind + 1)
    }
    if (resultEdata.hasNext || ind >= 0){
      throw new IllegalStateException(s"impossible, two iterator should end at the same time ${ind}, ${resultEdata.hasNext}")
    }
    val time2 = System.nanoTime()
    log.info(s"[Perf: ] map edge iterator cost ${(time2 - time0)/1000000} ms, in which iterating cost ${(time1 - time0) / 1000000} ms")
    this.withNewEdata(newData)
  }

  def mapTriplets[ED2: ClassTag](f: EdgeTriplet[VD,ED] => ED2, innerVertexDataStore: VertexDataStore[VD], outerVertexDataStore: VertexDataStore[VD], tripletFields: TripletFields): GrapeEdgePartition[VD, ED2] = {
    val time0 = System.nanoTime()
    val newData = new ArrayWithOffset[ED2](activeEdgeSet.startBit, activeEdgeSet.size)
    graphStructure.iterateTriplets(startLid, endLid, f,innerVertexDataStore,outerVertexDataStore, edatas, activeEdgeSet, edgeReversed,tripletFields.useSrc, tripletFields.useDst, newData)
    val time1 = System.nanoTime()
    log.info(s"[Perf:] mapping over triplets cost ${(time1 - time0)/1000000} ms")
    this.withNewEdata(newData)
  }

  def mapTriplets[ED2: ClassTag](f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2], innerVertexDataStore: VertexDataStore[VD], outerVertexDataStore : VertexDataStore[VD], includeSrc  : Boolean = true, includeDst : Boolean = true): GrapeEdgePartition[VD, ED2] = {
    val newData = new ArrayWithOffset[ED2](activeEdgeSet.startBit, activeEdgeSet.size)
    val time0 = System.nanoTime()
    val iter = tripletIterator(innerVertexDataStore,outerVertexDataStore,includeSrc,includeDst,reuseTriplet = true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    val resultEdata = f(pid, iter)
    val eids = graphStructure.getEids
    var ind = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
    while (ind >= 0 && resultEdata.hasNext){
      val eid = eids(ind)
      newData(eid.toInt) = resultEdata.next()
      ind = activeEdgeSet.nextSetBit(ind + 1)
    }
    if (ind >=0 || resultEdata.hasNext){
      throw new IllegalStateException(s"impossible, two iterator should end at the same time ${ind}, ${resultEdata.hasNext}")
    }
    val time1 = System.nanoTime()
    log.info(s"[Perf:] mapping over triplets iterator cost ${(time1 - time0)/1000000} ms")
    this.withNewEdata(newData)
  }

  def scanEdgeTriplet[A: ClassTag](innerVertexDataStore: VertexDataStore[VD],outerVertexDataStore: VertexDataStore[VD], sendMsg: EdgeContext[VD, ED, A] => Unit, mergeMsg: (A, A) => A, tripletFields: TripletFields, directionOpt : Option[(EdgeDirection)]) : Iterator[(VertexId, A)] ={
    val aggregates = new Array[A](innerVertexDataStore.size)
    val bitset = new BitSet(aggregates.length)

    val ctx = new EdgeContextImpl[VD, ED, A](mergeMsg, aggregates, bitset)
    val tripletIter = tripletIterator(innerVertexDataStore,outerVertexDataStore, tripletFields.useSrc, tripletFields.useDst, reuseTriplet = true,includeLid = true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    while (tripletIter.hasNext){
      val triplet = tripletIter.next()
      ctx.set(triplet.srcId, triplet.dstId, triplet.srcLid.toInt, triplet.dstLid.toInt, triplet.srcAttr,triplet.dstAttr, triplet.attr)
      sendMsg(ctx)
    }

    val curFid = graphStructure.fid()
    val idParser = new IdParser(graphStructure.fnum())
    bitset.iterator.map( localId => (idParser.generateGlobalId(curFid, localId), aggregates(localId)))
  }

  def reverse: GrapeEdgePartition[VD, ED] = {
    new GrapeEdgePartition[VD,ED](pid, localId,startLid,endLid, graphStructure, client,edatas,!edgeReversed, activeEdgeSet)
  }

  def withNewEdata[ED2: ClassTag](newEdata : ArrayWithOffset[ED2]): GrapeEdgePartition[VD, ED2] = {
    log.info(s"Creating new edge partition with new edge of size ${newEdata.length}, out edge num ${graphStructure.getOutEdgesNum}")
    new GrapeEdgePartition[VD,ED2](pid,localId,startLid,endLid,  graphStructure, client,newEdata, edgeReversed, activeEdgeSet)
  }

  def withNewMask(newActiveSet: BitSetWithOffset) : GrapeEdgePartition[VD,ED] = {
    new GrapeEdgePartition[VD,ED](pid,localId,startLid,endLid, graphStructure, client,edatas, edgeReversed, newActiveSet)
  }

  /**  currently we only support inner join with same vertex map*/
  def innerJoin[ED2: ClassTag, ED3: ClassTag]
  (other: GrapeEdgePartition[_, ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgePartition[VD, ED3] = {
    if (this.graphStructure != other.graphStructure){
      throw new IllegalStateException("Currently we only support inner join with same index")
    }
    val newMask = this.activeEdgeSet & other.activeEdgeSet
    log.info(s"Inner join edgePartition 0 has ${this.activeEdgeSet.cardinality()} actives edges, the other has ${other.activeEdgeSet} active edges")
    log.info(s"after join ${newMask.cardinality()} active edges")
    val newEData = new ArrayWithOffset[ED3](activeEdgeSet.startBit, activeEdgeSet.size)
    val oldIter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    while (oldIter.hasNext){
      val oldEdge = oldIter.next()
      val oldIndex = oldEdge.offset.toInt
      if (newMask.get(oldIndex)){
        //FIXME: we should not use eid.
        newEData(oldEdge.offset.toInt) =  f(oldEdge.srcId, oldEdge.dstId, oldEdge.attr, other.edatas(oldEdge.offset.toInt)) //FIXME: edatas can be null
      }
    }

    new GrapeEdgePartition[VD,ED3](pid,localId,startLid,endLid, graphStructure, client,newEData,edgeReversed, newMask)
  }

  override def toString: String =  super.toString + "(pid=" + pid + ",local id" + localId +
    ", start lid" + startLid + ", end lid " + endLid + ",graph structure" + graphStructure +
    ",out edges num" + partOutEdgeNum + ", in edges num" + partInEdgeNum +")"

  override def index: PartitionID = pid
}

class GrapeEdgePartitionBuilder[VD: ClassTag, ED: ClassTag](val numPartitions : Int,val client : VineyardClient) extends Logging{
  val srcOids = ScalaFFIFactory.newLongVector
  val dstOids = ScalaFFIFactory.newLongVector
  val innerOidBuilder : ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
  val outerOidBuilder : ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
  var lists : ArrayBuffer[EdgeShuffleReceived[ED]] = ArrayBuffer.empty[EdgeShuffleReceived[ED]]
  //Concurrency control should be done by upper level
  def addEdges(edges : EdgeShuffleReceived[ED]) : Unit = {
    lists.+=(edges)
  }

  /**
   * @return the built local vertex map id.
   */
  def buildLocalVertexMap(pid : Int) : LocalVertexMap[Long,Long] = {
    //We need to get oid->lid mappings in this executor.
    val innerHashSet = new OpenHashSet[Long]
    val outerHashSet = new OpenHashSet[Long]
    val time0 = System.nanoTime()
    for (edgeShuffleReceive <- lists){
      if (edgeShuffleReceive != null){
        for (edgeShuffle <- edgeShuffleReceive.fromPid2Shuffle){
          if (edgeShuffle != null){
            val receivedOids = edgeShuffle.oids
            val receivedOuterIds = edgeShuffle.outerOids
            var i = 0
            while (i < receivedOids.length){
              innerHashSet.add(receivedOids(i))
              i += 1
            }
            i = 0
            while (i < receivedOuterIds.length){
              outerHashSet.add(receivedOuterIds(i))
              i += 1
            }
          }
        }
      }
    }
    log.info(s"Found totally ${innerHashSet.size} in ${ExecutorUtils.getHostName}")
    if (innerHashSet.size == 0 && outerHashSet.size == 0){
      log.info(s"partition ${pid} empty")
      return null
    }
    val time01 = System.nanoTime()
    innerOidBuilder.reserve(innerHashSet.size)
    val iter = innerHashSet.iterator
    while (iter.hasNext){
      innerOidBuilder.unsafeAppend(iter.next());
    }
    outerOidBuilder.reserve(outerHashSet.size)
    val outerIter = outerHashSet.iterator
    while (outerIter.hasNext){
      outerOidBuilder.unsafeAppend(outerIter.next())
    }
    val time1 = System.nanoTime()
    val localVertexMapBuilder = ScalaFFIFactory.newLocalVertexMapBuilder(client, innerOidBuilder, outerOidBuilder)
    val localVM = localVertexMapBuilder.seal(client).get()
    val time2 = System.nanoTime()
    log.info(s"${ExecutorUtils.getHostName}: Finish building local vm: ${localVM.id()}, ${localVM.getInnerVerticesNum}, " +
      s"select out vertices cost ${(time01 - time0) / 1000000} ms adding buf take ${(time1 - time01)/ 1000000} ms," +
      s"total building time ${(time2 - time0)/ 1000000} ms")
    localVM
  }

  /** The received edata arrays contains both in edges and out edges, but we only need these out edges's edata array */
  def buildEdataArray(eids : Array[Long], defaultED : ED) : Array[ED] = {
    if (defaultED != null && lists(0).getArrays._3(0) == null){
      val edgeNum = lists.map(_.totalSize()).sum
      Array.fill[ED](edgeNum.toInt)(defaultED)
    }
    else if (defaultED == null && lists(0).getArrays._3(0) != null){
      val allArrays = lists.flatMap(_.getArrays._3).toArray
      val len = allArrays.map(_.length).sum
      val edataArray = new Array[ED](len)
      //got all edge data array
      var ind = 0

      for (arr <- allArrays){
        var i = 0
        val t = arr.length
        while (i < t){
          edataArray(ind) = arr(eids(i).toInt)
          i += 1
          ind += 1
        }
      }
      edataArray
    }
    else {
      throw new IllegalStateException("not possible, default ed  and array is both null or bot not empty")
    }
  }

  def createEids(edgeNum : Int, oeBeginNbr : PropertyNbrUnit[Long]) : Array[Long] = {
    val res = new Array[Long](edgeNum)
    var i = 0
    while (i < edgeNum){
      res(i) = oeBeginNbr.eid()
      oeBeginNbr.addV(16)
      i += 1
    }
    res
  }

  // no edata building is needed, we only persist edata to c++ when we run pregel
  def buildCSR(globalVMID : Long,numExecutors : Int): (GraphXVertexMap[Long,Long], GraphXCSR[Long]) = {
    val time0 = System.nanoTime()
    val edgesNum = lists.map(shuffle => shuffle.totalSize()).sum
    log.info(s"Got totally ${lists.length}, edges ${edgesNum} in ${ExecutorUtils.getHostName}")
    srcOids.resize(edgesNum)
    dstOids.resize(edgesNum)
    log.info(s"Constructing csr with global vm ${globalVMID}")
    val graphxVertexMapGetter = ScalaFFIFactory.newVertexMapGetter()
    val graphxVertexMap = graphxVertexMapGetter.get(client, globalVMID).get()
    log.info(s"Got graphx vertex map: ${graphxVertexMap}, total vnum ${graphxVertexMap.getTotalVertexSize}, fid ${graphxVertexMap.fid()}/${graphxVertexMap.fnum()}")
    var ind = 0
    for (shuffle <- lists){
      val (srcArrays, dstArrays, _) = shuffle.getArrays
      var i = 0
      val outerArrayLimit = srcArrays.length
      while (i < outerArrayLimit){
        var j = 0
        val innerLimit = srcArrays(i).length
        require(dstArrays(i).length == innerLimit)
        val srcArray = srcArrays(i)
        val dstArray = dstArrays(i)
        while (j < innerLimit){
          srcOids.set(ind,srcArray(j))
          dstOids.set(ind,dstArray(j))
          j += 1
          ind += 1
        }
        i += 1
      }
    }
    log.info("Finish adding edges to builders")
    val graphxCSRBuilder = ScalaFFIFactory.newGraphXCSRBuilder(client)
    graphxCSRBuilder.loadEdges(srcOids,dstOids,graphxVertexMap,graphxVertexMap.fnum() / numExecutors)
    val graphxCSR = graphxCSRBuilder.seal(client).get()
    val time1 = System.nanoTime()
    log.info(s"Finish building graphx csr ${graphxVertexMap.fid()}, cost ${(time1 - time0)/1000000} ms")
    (graphxVertexMap,graphxCSR)
  }

  //call this to delete c++ ptr and release memory of arrow builders
  def clearBuilders() : Unit = {
    lists = null
  }
}
object GrapeEdgePartition extends Logging {
  val queue = new ArrayBlockingQueue[(GraphStructure,VineyardClient,ArrayWithOffset[_],InHeapVertexDataStore[_])](1024)
  var array = null.asInstanceOf[Array[GrapeEdgePartition[_,_]]]
  val cnt = new AtomicInteger(0)
  //edata.
  def push(in : (GraphStructure,VineyardClient,ArrayWithOffset[_],InHeapVertexDataStore[_])): Unit = {
    require(queue.offer(in))
  }

  def incCount : Unit = {
    cnt.addAndGet(1)
  }

  def get[VD: ClassTag, ED: ClassTag](pid : Int) : GrapeEdgePartition[VD,ED] = {
    if (array == null){
      val size = queue.size()
      if (size == cnt.get()){
        log.info(s"Totally ${size} ele in queue, registered partition num ${cnt.get()}")
        array = new Array[GrapeEdgePartition[VD,ED]](size).asInstanceOf[Array[GrapeEdgePartition[_,_]]]
        for (i <- array.indices){
          val tuple = queue.poll()
          array(i) = new GrapeEdgePartition[VD,ED](i, 0,0, tuple._1.getInnerVertexSize, tuple._1, tuple._2, tuple._3.asInstanceOf[ArrayWithOffset[ED]])
          GrapeVertexPartition.setOuterVertexStore(i, tuple._4)
        }
      }
      else {
        val registeredNum = cnt.get()
        val maxTimes = (registeredNum + size - 1) / size // the largest split num
        val otherTimes = maxTimes - 1
        val numLargestSplit = (registeredNum - (maxTimes - 1) * size)
        log.info(s"Totally ${size} ele in queue, registered partition num ${cnt.get()}, first ${numLargestSplit} frags are splited into ${maxTimes}, others are splited into ${maxTimes - 1} times")
        array = new Array[GrapeEdgePartition[VD,ED]](registeredNum).asInstanceOf[Array[GrapeEdgePartition[_,_]]]
        var i = 0
        for (i <- 0 until numLargestSplit){
          val tuple = queue.poll()
          require(tuple != null, "fetch null ele from queue")
          val totalIvnum = tuple._1.getInnerVertexSize
          val chunkSize = (totalIvnum + maxTimes - 1) / maxTimes
          for (j <- 0 until maxTimes){
            val startLid = Math.min(totalIvnum,chunkSize * j)
            val endLid = Math.min(totalIvnum, startLid + chunkSize)
            if (j == maxTimes - 1){
              require(endLid == totalIvnum)
            }
            array(i) = new GrapeEdgePartition[VD,ED](i,  j, startLid, endLid, tuple._1, tuple._2, tuple._3.asInstanceOf[ArrayWithOffset[ED]])
            GrapeVertexPartition.setOuterVertexStore(i, tuple._4)
          }
        }
        for (i <- 0 until (size - numLargestSplit)){
          val tuple = queue.poll()
          require(tuple != null, "fetch null ele from queue")
          val totalIvnum = tuple._1.getInnerVertexSize
          val chunkSize = (totalIvnum + otherTimes - 1) / otherTimes
          for (j <- 0 until otherTimes){
            val startLid = Math.min(totalIvnum,chunkSize * j)
            val endLid = Math.min(totalIvnum, startLid + chunkSize)
            if (j == otherTimes - 1){
              require(endLid == totalIvnum)
            }
            array(i) = new GrapeEdgePartition[VD,ED](i, j, startLid, endLid, tuple._1, tuple._2, tuple._3.asInstanceOf[ArrayWithOffset[ED]])
            GrapeVertexPartition.setOuterVertexStore(i, tuple._4)
          }
        }
      }
      require(queue.size() == 0, "queue shoud be empty now")
    }
    array(pid).asInstanceOf[GrapeEdgePartition[VD,ED]]
  }
}

private class EdgeContextImpl[VD, ED, A](
                                                 mergeMsg: (A, A) => A,
                                                 aggregates: Array[A],
                                                 bitset: BitSet)
  extends EdgeContext[VD, ED, A] {

  private[this] var _srcId: VertexId = _
  private[this] var _dstId: VertexId = _
  private[this] var _localSrcId: Int = _
  private[this] var _localDstId: Int = _
  private[this] var _srcAttr: VD = _
  private[this] var _dstAttr: VD = _
  private[this] var _attr: ED = _

  def set(
           srcId: VertexId, dstId: VertexId,
           localSrcId: Int, localDstId: Int,
           srcAttr: VD, dstAttr: VD,
           attr: ED): Unit = {
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _attr = attr
  }

  def setSrcOnly(srcId: VertexId, localSrcId: Int, srcAttr: VD): Unit = {
    _srcId = srcId
    _localSrcId = localSrcId
    _srcAttr = srcAttr
  }

  def setDest(dstId: VertexId, localDstId: Int, dstAttr: VD, attr: ED): Unit = {
    _dstId = dstId
    _localDstId = localDstId
    _dstAttr = dstAttr
    _attr = attr
  }

  override def srcId: VertexId = _srcId
  override def dstId: VertexId = _dstId
  override def srcAttr: VD = _srcAttr
  override def dstAttr: VD = _dstAttr
  override def attr: ED = _attr

  override def sendToSrc(msg: A): Unit = {
    send(_localSrcId, msg)
  }
  override def sendToDst(msg: A): Unit = {
    send(_localDstId, msg)
  }

  @inline private def send(localId: Int, msg: A): Unit = {
    if (bitset.get(localId)) {
      aggregates(localId) = mergeMsg(aggregates(localId), msg)
    } else {
      aggregates(localId) = msg
      bitset.set(localId)
    }
  }
}
