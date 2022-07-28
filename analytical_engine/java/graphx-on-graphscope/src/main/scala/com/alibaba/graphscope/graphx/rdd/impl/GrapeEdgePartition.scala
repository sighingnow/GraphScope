package com.alibaba.graphscope.graphx.rdd.impl

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.ds.{PropertyNbrUnit, Vertex}
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.graph.{GSEdgeTriplet, GraphStructure, ReusableEdge}
import com.alibaba.graphscope.graphx.shuffle.{EdgeShuffle, EdgeShuffleReceived}
import com.alibaba.graphscope.graphx.store.{AbstractDataStore, DataStore, InHeapDataStore, InHeapEdgeDataStore, OffHeapEdgeDataStore}
import com.alibaba.graphscope.graphx.utils.{BitSetWithOffset, EIDAccessor, ExecutorUtils, GrapeUtils, IdParser, ScalaFFIFactory, ThreadSafeOpenHashSet}
import com.alibaba.graphscope.utils.{FFITypeFactoryhelper, ThreadSafeBitSet}
import org.apache.spark.Partition
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * [startLid, endLid), endLid is exclusive
 * FIXME: should not be serializable
 */
class GrapeEdgePartition[VD: ClassTag, ED: ClassTag](val pid : Int,
                                                     val localId : Int, // the order of partition in this fragment.
                                                     val localNum : Int, // how many part in this frag.
                                                     val startLid : Long,
                                                     val endLid : Long,
                                                     var activeEdgeNum : Int = 0, //use a instant variable to accelerate count.
                                                     val graphStructure: GraphStructure,
                                                     val client : VineyardClient,
                                                     var edatas : AbstractDataStore[ED],
                                                     //the original edatas are fully-specified,i.e. length equal to oeEdgeNum. but later we will use array with offset.
                                                     val edgeReversed : Boolean = false,
                                                     var activeEdgeSet : BitSetWithOffset = null) extends Logging  with Partition{
//  val startLid = 0
//  val endLid : Long = graphStructure.getInnerVertexSize
  val getVertexSize = endLid - startLid

  /** regard to our design, the card(activeEdgeSet) == partOutEdgeNum, len(edatas) == allEdgesNum, indexed by eid */
  val partOutEdgeNum : Long = graphStructure.getOutEdgesNum
  val partInEdgeNum : Long = graphStructure.getInEdgesNum
  val totalFragEdgeNum : Long = graphStructure.getTotalEdgesNum


  if (activeEdgeSet == null){
    val (startOffset,endOffset) = graphStructure.getOEOffsetRange(startLid,endLid)
    activeEdgeSet = new BitSetWithOffset(startBit = startOffset.toInt,endBit = endOffset.toInt)
    activeEdgeSet.setRange(startOffset.toInt, endOffset.toInt)
    activeEdgeNum = endOffset.toInt - startOffset.toInt
  }
  val NBR_SIZE = 16L
  //to avoid the difficult to get srcLid in iterating over edges.

//  log.info(s"Got edge partition ${this.toString}")


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

  def tripletIterator(innerVertexDataStore: DataStore[VD],
                      includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false, includeLid : Boolean = false)
  : Iterator[EdgeTriplet[VD, ED]] = graphStructure.tripletIterator(startLid,endLid,innerVertexDataStore,edatas,activeEdgeSet,edgeReversed,includeSrc,includeDst, reuseTriplet, includeLid)

  def filter(
              epred: EdgeTriplet[VD, ED] => Boolean,
              vpred: (VertexId, VD) => Boolean,
              innerVertexDataStore: DataStore[VD]): GrapeEdgePartition[VD, ED] = {
    //First invalided all invalid edges from invalid vertices.
    val tripletIter = tripletIterator(innerVertexDataStore, true, true, true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
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
//    val newEdata = new ArrayWithOffset[ED](activeEdgeSet.startBit,activeEdgeSet.size)// new Array[ED](allEdgesNum.toInt)
    val newEdata = edatas.getOrCreate[ED]
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
          newEdata.setData(prevEdgeInd.toInt, attrSum)
          log.info(s"end of acculating edge ${curSrcId}, ${curDstId}, ${attrSum}")
        }
        curSrcId = edge.srcId
        curDstId = edge.dstId
        prevEdgeInd = curIndex
        attrSum = edge.attr
      }
      flag = true
    }
    new GrapeEdgePartition[VD,ED](pid,localId,localNum, startLid, endLid, newMask.cardinality(), graphStructure, client,newEdata, edgeReversed, newMask)
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[VD, ED2] = {
//    val newData = new ArrayWithOffset[ED2](activeEdgeSet.startBit, activeEdgeSet.size)// new Array[ED2](allEdgesNum.toInt)
    val newData = edatas.getOrCreate[ED2]
    graphStructure.iterateEdges(startLid, endLid,f,edatas, activeEdgeSet, edgeReversed, newData)
    this.withNewEdata(newData)
  }

  def emptyIteration : Unit = {
//    val newEdata = edatas.getOrCreate[ED]
    graphStructure.emptyIterateEdges[ED](startLid, endLid, edatas,activeEdgeSet, edgeReversed, edatas)
  }
  def emptyIterationTriplet(innerVertexDataStore: DataStore[VD]) : Unit = {
//    val newEdata = edatas.getOrCreate[ED]
    graphStructure.emptyIterateTriplets[VD,ED](startLid, endLid, innerVertexDataStore,edatas,activeEdgeSet, edgeReversed,true, true, edatas)
  }

  def map[ED2: ClassTag](f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): GrapeEdgePartition[VD, ED2] = {
    val time0 = System.nanoTime()
//    val newData = new ArrayWithOffset[ED2](activeEdgeSet.startBit, activeEdgeSet.size)
    val newData = edatas.getOrCreate[ED2]
    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    val resultEdata = f(pid, iter)
    val time1 = System.nanoTime()
    var ind = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
    while (ind >= 0 && resultEdata.hasNext){
      newData.setData(ind,resultEdata.next())
      ind = activeEdgeSet.nextSetBit(ind + 1)
    }
    if (resultEdata.hasNext || ind >= 0){
      throw new IllegalStateException(s"impossible, two iterator should end at the same time ${ind}, ${resultEdata.hasNext}")
    }
    val time2 = System.nanoTime()
//    log.info(s"[Perf: ] map edge iterator cost ${(time2 - time0)/1000000} ms, in which iterating cost ${(time1 - time0) / 1000000} ms")
    this.withNewEdata(newData)
  }

  def mapTriplets[ED2: ClassTag](f: EdgeTriplet[VD,ED] => ED2, innerVertexDataStore: DataStore[VD], tripletFields: TripletFields): GrapeEdgePartition[VD, ED2] = {
    val time0 = System.nanoTime()
//    val newData = new ArrayWithOffset[ED2](activeEdgeSet.startBit, activeEdgeSet.size)
    val newData = edatas.getOrCreate[ED2]
    val time01 = System.nanoTime()
    graphStructure.iterateTriplets(startLid, endLid, f,innerVertexDataStore, edatas, activeEdgeSet, edgeReversed,tripletFields.useSrc, tripletFields.useDst, newData)
    val time1 = System.nanoTime()
    log.info(s"[Perf:] part ${pid}, lid ${localId}, mapping over triplets size ${activeEdgeNum} cost ${(time1 - time0)/1000000} ms, create array cost ${(time01 - time0)/1000000}ms")
    this.withNewEdata(newData)
  }

  def mapTriplets[ED2: ClassTag](f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2], innerVertexDataStore: DataStore[VD], includeSrc  : Boolean = true, includeDst : Boolean = true): GrapeEdgePartition[VD, ED2] = {
//    val newData = new ArrayWithOffset[ED2](activeEdgeSet.startBit, activeEdgeSet.size)
    val newData = edatas.getOrCreate[ED2]
    val time0 = System.nanoTime()
    val iter = tripletIterator(innerVertexDataStore,includeSrc,includeDst,reuseTriplet = true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    val resultEdata = f(pid, iter)
    var ind = activeEdgeSet.nextSetBit(activeEdgeSet.startBit)
    while (ind >= 0 && resultEdata.hasNext){
      newData.setData(ind,resultEdata.next())
      ind = activeEdgeSet.nextSetBit(ind + 1)
    }
    if (ind >=0 || resultEdata.hasNext){
      throw new IllegalStateException(s"impossible, two iterator should end at the same time ${ind}, ${resultEdata.hasNext}")
    }
    val time1 = System.nanoTime()
//    log.info(s"[Perf:] mapping over triplets iterator cost ${(time1 - time0)/1000000} ms")
    this.withNewEdata(newData)
  }

  def scanEdgeTriplet[A: ClassTag](innerVertexDataStore: DataStore[VD], sendMsg: EdgeContext[VD, ED, A] => Unit, mergeMsg: (A, A) => A, tripletFields: TripletFields, directionOpt : Option[(EdgeDirection)]) : Iterator[(VertexId, A)] ={
    val aggregates = new Array[A](innerVertexDataStore.size)
    val bitset = new BitSet(aggregates.length)

    val ctx = new EdgeContextImpl[VD, ED, A](mergeMsg, aggregates, bitset)
    val tripletIter = tripletIterator(innerVertexDataStore, tripletFields.useSrc, tripletFields.useDst, reuseTriplet = true,includeLid = true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
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
    new GrapeEdgePartition[VD,ED](pid, localId,localNum,startLid,endLid,activeEdgeNum, graphStructure, client,edatas,!edgeReversed, activeEdgeSet)
  }

  def withNewEdata[ED2: ClassTag](newEdata : AbstractDataStore[ED2]): GrapeEdgePartition[VD, ED2] = {
//    log.info(s"Creating new edge partition with new edge of size ${newEdata.length}, out edge num ${graphStructure.getOutEdgesNum}")
    new GrapeEdgePartition[VD,ED2](pid,localId,localNum,startLid,endLid,activeEdgeNum,  graphStructure, client,newEdata, edgeReversed, activeEdgeSet)
  }

  def withNewMask(newActiveSet: BitSetWithOffset) : GrapeEdgePartition[VD,ED] = {
    new GrapeEdgePartition[VD,ED](pid,localId,localNum,startLid,endLid, newActiveSet.cardinality(), graphStructure, client,edatas, edgeReversed, newActiveSet)
  }

  /**  currently we only support inner join with same vertex map*/
  def innerJoin[ED2: ClassTag, ED3: ClassTag]
  (other: GrapeEdgePartition[_, ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgePartition[VD, ED3] = {
    if (this.graphStructure != other.graphStructure){
      throw new IllegalStateException("Currently we only support inner join with same index")
    }
    val newMask = this.activeEdgeSet & other.activeEdgeSet
    val newActiveEdgesNum = newMask.cardinality()
    log.info(s"Inner join edgePartition 0 has ${this.activeEdgeSet.cardinality()} actives edges, the other has ${other.activeEdgeSet} active edges")
    log.info(s"after join ${newMask.cardinality()} active edges")
//    val newEData = new ArrayWithOffset[ED3](activeEdgeSet.startBit, activeEdgeSet.size)
    val newEData = edatas.getOrCreate[ED3]
    val oldIter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    while (oldIter.hasNext){
      val oldEdge = oldIter.next()
      val oldIndex = oldEdge.offset.toInt
      if (newMask.get(oldIndex)){
        //FIXME: we should not use eid.
        newEData.setData(oldEdge.offset.toInt, f(oldEdge.srcId, oldEdge.dstId, oldEdge.attr, other.edatas.getData(oldEdge.offset.toInt))) //FIXME: edatas can be null
      }
    }

    new GrapeEdgePartition[VD,ED3](pid,localId,localNum,startLid,endLid, newActiveEdgesNum, graphStructure, client,newEData,edgeReversed, newMask)
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

  def collectOids(inner: ThreadSafeBitSet, outer: ThreadSafeBitSet, shuffles: ArrayBuffer[EdgeShuffle[_, _]], cores: Int) : Unit = {
    val time0 = System.nanoTime()
    val atomicInt = new AtomicInteger(0)
    val threads = new Array[Thread](cores)
    val numShuffles = shuffles.length
    var tid = 0
    while (tid < cores) {
      val threadId= tid
      val newThread = new Thread() {
        override def run(): Unit = {
          var flag = true
          while (flag) {
            val got = atomicInt.getAndAdd(1);
            if (got >= numShuffles) {
              flag = false
            }
            else {
              val edgeShuffle = shuffles(got)
              if (edgeShuffle.oids != null && edgeShuffle.outerOids != null) {
//                log.info(s"Thread ${threadId} got shuffle id ${got} ${edgeShuffle}")
                val receivedOids = edgeShuffle.oids
                val receivedOuterIds = edgeShuffle.outerOids
                var i = 0
                while (i < receivedOids.length) {
                  inner.set(receivedOids(i).toInt)
                  i += 1
                }
                i = 0
                while (i < receivedOuterIds.length) {
                  outer.set(receivedOuterIds(i).toInt)
                  i += 1
                }
              }
            }
          }
        }
      }
      newThread.start()
      threads(tid) = newThread
      tid += 1
    }
    for (i <- 0 until cores) {
      threads(i).join()
    }

    val time1 = System.nanoTime()
    log.info(s"[Collect oids cost ${(time1 - time0)/1000000}ms]")
  }

  /**
   * @return the built local vertex map id.
   */
  def buildLocalVertexMap(pid : Int, parallelism : Int) : LocalVertexMap[Long,Long] = {
    //We need to get oid->lid mappings in this executor.
    val time0 = System.nanoTime()
    val edgeShuffles = new ArrayBuffer[EdgeShuffle[_,_]]()
    var maxOid = 0L
    for (edgeShuffleReceive <- lists){
      if (edgeShuffleReceive != null){
        for (edgeShuffle <- edgeShuffleReceive.fromPid2Shuffle){
          if (edgeShuffle != null){
            edgeShuffles.+=(edgeShuffle)
            maxOid = Math.max(maxOid, edgeShuffle.maxOid)
          }
        }
      }
    }
    log.info(s"part ${pid} max oid ${maxOid}, parallelism ${parallelism}")
//    val innerHashSet = new ThreadSafeOpenHashSet[Long](innerOidSize / 32)
//    val outerHashSet = new ThreadSafeOpenHashSet[Long](outerOidSize / 32)
//    val innerHashSet = new OpenHashSet[Long](innerOidSize / 20)
//    val outerHashSet = new OpenHashSet[Long](outerOidSize / 20)
    val innerBitSet = new ThreadSafeBitSet()
    val outerBitSet = new ThreadSafeBitSet()
    collectOids(innerBitSet, outerBitSet, edgeShuffles, parallelism)

    log.info(s"Found totally inner ${innerBitSet.cardinality()}, outer ${outerBitSet.cardinality()} in ${ExecutorUtils.getHostName}:${pid}")
    if (innerBitSet.cardinality() == 0 && outerBitSet.cardinality() == 0){
      log.info(s"partition ${pid} empty")
      return null
    }
    val time01 = System.nanoTime()
    innerOidBuilder.reserve(innerBitSet.cardinality())
    var cur = innerBitSet.nextSetBit(0)
    while (cur >= 0){
      innerOidBuilder.unsafeAppend(cur)
      cur = innerBitSet.nextSetBit(cur + 1)
    }
    outerOidBuilder.reserve(outerBitSet.cardinality())
    cur = outerBitSet.nextSetBit(0)
    while (cur >= 0){
      outerOidBuilder.unsafeAppend(cur)
      cur = outerBitSet.nextSetBit(cur + 1)
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
  def buildEdataStore(defaultED : ED, totalEdgeNum : Int, client : VineyardClient, eidAccessor : EIDAccessor, numSplit : Int = 1) : AbstractDataStore[ED] = {
    val eDataStore = if (GrapeUtils.isPrimitive[ED]){
      new OffHeapEdgeDataStore[ED](totalEdgeNum,client,numSplit,eidAccessor)
    }
    else {
      val edataArray = buildArrayStore(defaultED,totalEdgeNum)
      new InHeapEdgeDataStore[ED](totalEdgeNum, client, numSplit, edataArray,eidAccessor)
    }
    fillEdataStore(defaultED,totalEdgeNum,eDataStore)
    eDataStore
  }

  def buildArrayStore(defaultED : ED, totalEdgeNum : Long) : Array[ED] = {
    if (defaultED != null && lists(0).getArrays._3(0) == null){
      //      val edgeNum = lists.map(_.totalSize()).sum
      Array.fill[ED](totalEdgeNum.toInt)(defaultED)
    }
    else if (defaultED == null && lists(0).getArrays._3(0) != null){
      val allArrays = lists.flatMap(_.getArrays._3).toArray
      val rawEdgesNum = allArrays.map(_.length).sum
      require(rawEdgesNum == totalEdgeNum, s"edge num neq ${rawEdgesNum}, ${totalEdgeNum}")
      //      log.info(s"eids size ${eids.length}, raw edge num ${rawEdgesNum}")
      val edataArray = new Array[ED](rawEdgesNum)
      //flat array
      var ind = 0
      for (arr <- allArrays){
        var i = 0
        val t = arr.length
        while (i < t){
          edataArray(ind) = arr(i)
          i += 1
          ind += 1
        }
      }
      edataArray
    }
    else {
      throw new IllegalStateException("not possible, default ed and array is both null or bot not empty")
    }
  }
  def fillEdataStore(defaultED : ED, totalEdgeNum : Int, edataStore : DataStore[ED]) : Unit = {
    val arrayBuilder = edataStore.asInstanceOf[OffHeapEdgeDataStore[ED]].arrayBuilder
    if (defaultED != null && lists(0).getArrays._3(0) == null){
      var i = 0
      while (i < totalEdgeNum){
        arrayBuilder.set(i, defaultED)
        i += 1
      }
    }
    else if (defaultED == null && lists(0).getArrays._3(0) != null){
      val allArrays = lists.flatMap(_.getArrays._3).toArray
      val rawEdgesNum = allArrays.map(_.length).sum
      require(rawEdgesNum == totalEdgeNum, s"edge num neq ${rawEdgesNum}, ${totalEdgeNum}")
      var ind = 0
      for (arr <- allArrays){
        var i = 0
        val t = arr.length
        while (i < t){
//          edataArray(ind) = arr(i)
          arrayBuilder.set(ind, arr(i))
          i += 1
          ind += 1
        }
      }
    }
    else {
      throw new IllegalStateException("not possible, default ed and array is both null or bot not empty")
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
    require(ind == edgesNum, s"after iterating over edge shuffle, ind ${ind} neq ${edgesNum}")
    log.info("Finish adding edges to builders")
    val graphxCSRBuilder = ScalaFFIFactory.newGraphXCSRBuilder(client)
    log.info(s"building csr with local num ${graphxVertexMap.fnum()}/${numExecutors}")
    require(srcOids.size() == edgesNum && dstOids.size() == edgesNum, s"src size ${srcOids.size()} dst size ${dstOids.size()}, edgenum ${edgesNum}")
    graphxCSRBuilder.loadEdges(srcOids,dstOids,graphxVertexMap,graphxVertexMap.fnum() / numExecutors)
    val graphxCSR = graphxCSRBuilder.seal(client).get()
    val time1 = System.nanoTime()
    log.info(s"Finish building graphx csr ${graphxVertexMap.fid()}, cost ${(time1 - time0)/1000000} ms")
    (graphxVertexMap,graphxCSR)
  }

  def fillVertexData(innerVertexData : DataStore[VD], graphStructure: GraphStructure) : Unit = {
    for (edgeShuffleReceived <- lists){
      for (shuffle <- edgeShuffleReceived.fromPid2Shuffle){
        val edgeShuffle = shuffle.asInstanceOf[EdgeShuffle[VD,_]]
        val innerOids = edgeShuffle.oids
        val innerVertexAttrs = edgeShuffle.innerVertexAttrs
        val outerOids = edgeShuffle.outerOids
        val outerVertexAttrs = edgeShuffle.outerVertexAttrs
        if (innerVertexAttrs == null || innerVertexAttrs.length == 0){
//          log.info("no vertex attrs found in shuffle")
        }
        else {
          var i = 0
          var limit = innerOids.length
          val grapeVertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
          require(limit == innerVertexAttrs.length, s"size neq ${limit}, ${innerVertexAttrs.length}")
          val ivnum = graphStructure.getInnerVertexSize.toInt
          while (i < limit){
            val oid = innerOids(i)
            val vdata = innerVertexAttrs(i)
            require(graphStructure.getInnerVertex(oid,grapeVertex))
            val lid = grapeVertex.GetValue().toInt
            require(lid < ivnum, s"expect no outer vertex ${lid}, ${ivnum}")
            innerVertexData.setData(lid, vdata)
            i += 1
          }
          //map outer vertices
          i = 0
          limit = outerOids.length
          require(limit == outerVertexAttrs.length, s"size neq ${limit}, ${outerVertexAttrs.length}")
          while (i < limit){
            val oid = outerOids(i)
            val vdata = outerVertexAttrs(i)
            require(graphStructure.getOuterVertex(oid,grapeVertex))
            val lid = grapeVertex.GetValue().toInt
            require(lid >= ivnum, s"expect no outer vertex ${lid}, ${ivnum}")
            innerVertexData.setData(lid, vdata)
            i += 1
          }
        }
      }
    }
  }

  //call this to delete c++ ptr and release memory of arrow builders
  def clearBuilders() : Unit = {
    lists = null
  }

}
object GrapeEdgePartition extends Logging {
  val tupleQueue = new ArrayBlockingQueue[(Int,GraphStructure,VineyardClient,AbstractDataStore[_],InHeapDataStore[_])](1024)
  val pidQueue = new ArrayBlockingQueue[Int](1024)
  var pid2EdgePartition = null.asInstanceOf[mutable.HashMap[Int,GrapeEdgePartition[_,_]]]
  //edata.
  def push(in : (Int,GraphStructure,VineyardClient,AbstractDataStore[_],InHeapDataStore[_])): Unit = {
    require(tupleQueue.offer(in))
  }

  def incCount(pid : Int) : Unit = synchronized{
    require(pidQueue.offer(pid))
  }

  def createPartitions[VD: ClassTag, ED: ClassTag](pid : Int, totalPartitionNum : Int) : Unit = synchronized{
    if (pid2EdgePartition == null){
      synchronized {
        if (pid2EdgePartition == null){
          val size = tupleQueue.size()
          pid2EdgePartition = new mutable.HashMap[Int,GrapeEdgePartition[VD,ED]].asInstanceOf[mutable.HashMap[Int,GrapeEdgePartition[_,_]]]
          if (size == pidQueue.size()){
            log.info(s"Totally $size ele in queue, registered partition num ${pidQueue.size()}")
            for (_ <- 0 until size){
              val tuple = tupleQueue.poll()
              tuple._4.setNumSplit(1)
              val edataStore = tuple._4.asInstanceOf[AbstractDataStore[ED]]
              val _ = pidQueue.poll()
              pid2EdgePartition(tuple._1) = new GrapeEdgePartition[VD,ED](tuple._1, 0, 1, 0,  tuple._2.getInnerVertexSize,tuple._2.getOutEdgesNum.toInt, tuple._2, tuple._3, edataStore)
              val innerStore = tuple._5
              innerStore.setNumSplit(1)
              GrapeVertexPartition.setVertexStore(tuple._1,innerStore)
//              GrapeVertexPartition.setOuterVertexStore(tuple._1, tuple._6)
            }
          }
          else {
            //find out the new partition ids which didn't appear in frag queue.
            val candidates = new mutable.Queue[Int]
            val tuplePids = new BitSet(totalPartitionNum)
            tupleQueue.forEach(tuple => tuplePids.set(tuple._1))
            pidQueue.forEach(p => if (!tuplePids.get(p)) candidates.enqueue(p))
            log.info(s"candidates num ${candidates.size}")
            require(candidates.size == (pidQueue.size() - tupleQueue.size()), s"candidates size ${candidates.size}, neq ${pidQueue.size()} - ${tupleQueue.size()}")
            val registeredNum = pidQueue.size()
            val maxTimes = (registeredNum + size - 1) / size // the largest split num
            val numLargestSplit = (registeredNum - (maxTimes - 1) * size)

            log.info(s"Totally ${size} ele in queue, registered partition num ${pidQueue.size()}, first ${numLargestSplit} frags are splited into ${maxTimes}, others are splited into ${maxTimes - 1} times")
            for (i <- 0 until size){
              val tuple = tupleQueue.poll()
              val totalIvnum = tuple._2.getInnerVertexSize
              val times = {
                if (i < numLargestSplit) maxTimes
                else maxTimes - 1
              }
              val chunkSize = (totalIvnum + times - 1) / times
              //first set the first one

              val graphStructure = tuple._2
              //split into partitions where each partition has rarely even number of edges.
              val ranges = splitFragAccordingToEdges(graphStructure, times)
              for (j <- 0 until times){
                val startLid = ranges(j)._1
                val endLid = ranges(j)._2
                if (j == times - 1){
                  require(endLid == totalIvnum)
                }
                val innerStore = tuple._5
                innerStore.setNumSplit(times)
                val edataStore = tuple._4.asInstanceOf[AbstractDataStore[ED]]
                edataStore.setNumSplit(times)
                if (j == 0){
                  pid2EdgePartition(tuple._1) = new GrapeEdgePartition[VD,ED](tuple._1,  j, times, startLid, endLid,0, graphStructure, tuple._3,edataStore)
                  GrapeVertexPartition.setVertexStore(tuple._1, innerStore)
//                  GrapeVertexPartition.setOuterVertexStore(tuple._1, tuple._6)
                  log.info(s"creating partition for pid ${tuple._1}, (${startLid},${endLid}), fid ${graphStructure.fid()}")
                }
                else {
                  val dstPid = candidates.dequeue()
                  pid2EdgePartition(dstPid) = new GrapeEdgePartition[VD,ED](dstPid,  j, times, startLid, endLid,0, graphStructure, tuple._3, edataStore)
                  GrapeVertexPartition.setVertexStore(dstPid, innerStore)
//                  GrapeVertexPartition.setOuterVertexStore(dstPid, tuple._6)
                  log.info(s"creating partition for pid ${dstPid}, (${startLid},${endLid}), fid ${graphStructure.fid()}")
                }
              }
            }
          }
          require(tupleQueue.size() == 0, "queue shoud be empty now")
        }
        else {
          log.info(s"partition ${pid} skip building since array is already created")
        }
      }
    }
  }

  def get[VD: ClassTag, ED: ClassTag](pid : Int) : GrapeEdgePartition[VD,ED] = synchronized{
    require(pid2EdgePartition != null, "call create partitions first")
    pid2EdgePartition(pid).asInstanceOf[GrapeEdgePartition[VD,ED]]
  }

  def splitFragAccordingToEdges(graphStructure: GraphStructure, numPart : Int) : Array[(Int,Int)] = {
    val numEdges = graphStructure.getOutEdgesNum
    val edgesPerSplit = (numEdges + numPart - 1) / numPart
    var curLid = 0
    val res = new Array[(Int,Int)](numPart)
    for (i <- 0 until numPart){
      val targetOffset = Math.min(edgesPerSplit * (i + 1), numEdges)
      val beginLid = curLid
      while (graphStructure.getOEBeginOffset(curLid) < targetOffset){
        curLid += 1
      }
      if (i == numPart - 1){
        curLid = Math.max(curLid, graphStructure.getInnerVertexSize.toInt)
      }
      res(i) = (beginLid, curLid)
      log.info(s"For part ${i}, startLid ${beginLid}, endLid${curLid}, num edges in this part ${graphStructure.getOEBeginOffset(curLid) - graphStructure.getOEBeginOffset(beginLid)}, total edges ${numEdges}, edges per split ${edgesPerSplit}")
    }
    //it is possible that the last vertices has no out edges.
    require(curLid == graphStructure.getInnerVertexSize, s"after split, should iterate over all ivertex ${curLid}, ${graphStructure.getInnerVertexSize}")
    res
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
