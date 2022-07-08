package com.alibaba.graphscope.graphx.rdd.impl

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.graph.{GSEdgeTriplet, GraphStructure, ReusableEdge}
import com.alibaba.graphscope.graphx.shuffle.EdgeShuffleReceived
import com.alibaba.graphscope.graphx.store.VertexDataStore
import com.alibaba.graphscope.graphx.utils.{ExecutorUtils, IdParser, ScalaFFIFactory}
import org.apache.spark.Partition
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * [startLid, endLid), endLid is exclusive
 * FIXME: should not be serializable
 */
class GrapeEdgePartition[VD: ClassTag, ED: ClassTag](val pid : Int,
                                                     val graphStructure: GraphStructure,
                                                     val client : VineyardClient,
                                                     var edatas : Array[ED],
                                                     val edgeReversed : Boolean = false,
                                                     var activeEdgeSet : BitSet = null) extends Logging  with Partition{
  private val serialVersionUID = 6529685098267757690L
  val startLid = 0
  val endLid : Long = graphStructure.getInnerVertexSize

  /** regard to our design, the card(activeEdgeSet) == partOutEdgeNum, len(edatas) == allEdgesNum, indexed by eid */
  val partOutEdgeNum : Long = graphStructure.getOutEdgesNum
  val partInEdgeNum : Long = graphStructure.getInEdgesNum
  val allEdgesNum = partInEdgeNum + partOutEdgeNum


  if (activeEdgeSet == null){
    activeEdgeSet = new BitSet(partOutEdgeNum.toInt) // just need to control out edges
    activeEdgeSet.setUntil(partOutEdgeNum.toInt)
  }
  val NBR_SIZE = 16L
  //to avoid the difficult to get srcLid in iterating over edges.

  log.info(s"Got edge partition ${this.toString}")


  def getDegreeArray(edgeDirection: EdgeDirection): Array[Int] = {
    if (edgeDirection.equals(EdgeDirection.In)){
      graphStructure.inDegreeArray
    }
    else if (edgeDirection.equals(EdgeDirection.Out)){
      graphStructure.outDegreeArray
    }
    else{
      graphStructure.inOutDegreeArray
    }
  }

  /** Iterate over out edges only, in edges will be iterated in other partition */
  def iterator : Iterator[Edge[ED]] = {
    graphStructure.iterator(edatas,activeEdgeSet,edgeReversed)
  }

  def tripletIterator(vertexDataStore: VertexDataStore[VD],
                       includeSrc: Boolean = true, includeDst: Boolean = true, reuseTriplet : Boolean = false, includeLid : Boolean = false)
  : Iterator[EdgeTriplet[VD, ED]] = graphStructure.tripletIterator(vertexDataStore,edatas,activeEdgeSet,edgeReversed,includeSrc,includeDst, reuseTriplet, includeLid)

  def filter(
              epred: EdgeTriplet[VD, ED] => Boolean,
              vpred: (VertexId, VD) => Boolean,
              vertexDataStore: VertexDataStore[VD]): GrapeEdgePartition[VD, ED] = {
    //First invalided all invalid edges from invalid vertices.
    val tripletIter = tripletIterator(vertexDataStore, true, true, true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    val newActiveEdges = new BitSet(partOutEdgeNum.toInt)
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
    val newMask = new BitSet(activeEdgeSet.capacity)
    newMask.union(activeEdgeSet)
    val newEdata = new Array[ED](allEdgesNum.toInt)
    while (iter.hasNext){
      val edge = iter.next()
      val curIndex = edge.eid
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
    new GrapeEdgePartition[VD,ED](pid, graphStructure, client,newEdata, edgeReversed, newMask)
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[VD, ED2] = {
    val newData = new Array[ED2](allEdgesNum.toInt)
    graphStructure.iterateEdges(f,edatas, activeEdgeSet, edgeReversed, newData)
//    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
//    var ind = 0;
//    while (iter.hasNext){
//      val edge = iter.next()
//      newData(edge.eid.toInt) =  f(edge)
//      ind += 1
//    }
    this.withNewEdata(newData)
  }

  def map[ED2: ClassTag](f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): GrapeEdgePartition[VD, ED2] = {
    val time0 = System.nanoTime()
    val newData = new Array[ED2](allEdgesNum.toInt)
    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    val resultEdata = f(pid, iter)
    val time1 = System.nanoTime()
    val eids = graphStructure.getEids
    var ind = activeEdgeSet.nextSetBit(0);
    while (ind >= 0 && resultEdata.hasNext){
      val eid = eids(ind)
      newData(eid.toInt) = resultEdata.next()
      ind = activeEdgeSet.nextSetBit(ind + 1)
    }
    if (resultEdata.hasNext || ind >= 0){
      throw new IllegalStateException(s"impossible, two iterator should end at the same time ${ind}, ${resultEdata.hasNext}")
    }
    val time2 = System.nanoTime()
    log.info(s"[Perf: ] map edge iterator cost ${(time2 - time0)/1000000} ms, in which iterating cost ${(time1 - time0) / 1000000} ms")
    this.withNewEdata(newData)
  }

  def mapTriplets[ED2: ClassTag](f: EdgeTriplet[VD,ED] => ED2, vertexDataStore: VertexDataStore[VD], tripletFields: TripletFields): GrapeEdgePartition[VD, ED2] = {
    val time0 = System.nanoTime()
//    val newData = new Array[ED2](allEdgesNum.toInt)
//    val iter = tripletIterator(vertexDataStore, tripletFields.useSrc, tripletFields.useDst, reuseTriplet = true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
//    var ind = 0;
//    while (iter.hasNext){
//      val edge = iter.next()
//      newData(edge.eid.toInt) =  f(edge)
//      ind += 1
//    }
    val newData = new Array[ED2](allEdgesNum.toInt)
    graphStructure.iterateTriplets(f,vertexDataStore, edatas, activeEdgeSet, edgeReversed,tripletFields.useSrc, tripletFields.useDst, newData)
    val time1 = System.nanoTime()
    log.info(s"[Perf:] mapping over triplets cost ${(time1 - time0)/1000000} ms")
    this.withNewEdata(newData)
  }

  def mapTriplets[ED2: ClassTag](f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2], vertexDataStore: VertexDataStore[VD], includeSrc  : Boolean = true, includeDst : Boolean = true): GrapeEdgePartition[VD, ED2] = {
    val newData = new Array[ED2](allEdgesNum.toInt)
    //    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    val time0 = System.nanoTime()
    val iter = tripletIterator(vertexDataStore,includeSrc,includeDst,reuseTriplet = true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    val resultEdata = f(pid, iter)
    val eids = graphStructure.getEids
    var ind = activeEdgeSet.nextSetBit(0)
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

  def scanEdgeTriplet[A: ClassTag](vertexDataStore: VertexDataStore[VD], sendMsg: EdgeContext[VD, ED, A] => Unit, mergeMsg: (A, A) => A, tripletFields: TripletFields, directionOpt : Option[(EdgeDirection)]) : Iterator[(VertexId, A)] ={
    val aggregates = new Array[A](vertexDataStore.size.toInt)
    val bitset = new BitSet(aggregates.length)

    val ctx = new EdgeContextImpl[VD, ED, A](mergeMsg, aggregates, bitset)
    val tripletIter = tripletIterator(vertexDataStore, tripletFields.useSrc, tripletFields.useDst, reuseTriplet = true,includeLid = true).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
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
    new GrapeEdgePartition[VD,ED](pid, graphStructure, client,edatas,!edgeReversed, activeEdgeSet)
  }

  def withNewEdata[ED2: ClassTag](newEdata : Array[ED2]): GrapeEdgePartition[VD, ED2] = {
    log.info(s"Creating new edge partition with new edge of size ${newEdata.length}, out edge num ${graphStructure.getOutEdgesNum}")
    new GrapeEdgePartition[VD,ED2](pid,  graphStructure, client,newEdata, edgeReversed, activeEdgeSet)
  }

  def withNewMask(newActiveSet: BitSet) : GrapeEdgePartition[VD,ED] = {
    new GrapeEdgePartition[VD,ED](pid, graphStructure, client,edatas, edgeReversed, newActiveSet)
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
    val newEdata = new Array[ED3](allEdgesNum.toInt)
    val oldIter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    while (oldIter.hasNext){
      val oldEdge = oldIter.next()
      val oldIndex = oldEdge.offset.toInt
      if (newMask.get(oldIndex)){
        newEdata(oldEdge.eid.toInt) =  f(oldEdge.srcId, oldEdge.dstId, oldEdge.attr, other.edatas(oldEdge.eid.toInt)) //FIXME: edatas can be null
      }
    }

    new GrapeEdgePartition[VD,ED3](pid, graphStructure, client,newEdata,edgeReversed, newMask)
  }

  override def toString: String =  super.toString + "(pid=" + pid +
    ", start lid" + startLid + ", end lid " + endLid + ",graph structure" + graphStructure +
    ",out edges num" + partOutEdgeNum + ", in edges num" + partInEdgeNum +")"

  override def index: PartitionID = pid
}

class GrapeEdgePartitionBuilder[VD: ClassTag, ED: ClassTag](val numPartitions : Int,val client : VineyardClient) extends Logging{
//  val srcOidBuilder: ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
//  val dstOidBuilder: ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
  val srcOids = ScalaFFIFactory.newLongVector
  val dstOids = ScalaFFIFactory.newLongVector
//  val edataBuilder : ArrowArrayBuilder[ED] = ScalaFFIFactory.newArrowArrayBuilder(GrapeUtils.getRuntimeClass[ED].asInstanceOf[Class[ED]])
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
  def buildLocalVertexMap() : LocalVertexMap[Long,Long] = {
    //We need to get oid->lid mappings in this executor.
    val innerHashSet = new OpenHashSet[Long]
    for (edgeShuffleReceive <- lists){
      for (edgeShuffle <- edgeShuffleReceive.fromPid2Shuffle){
        log.info(s"edge shuffle ${edgeShuffle} size ${edgeShuffle.size()}")
        val receivedOids = edgeShuffle.oids
        log.info(s"Before union with ${receivedOids.size}, size ${innerHashSet.size}")
        var i = 0
        while (i < receivedOids.length){
          innerHashSet.add(receivedOids(i))
          i += 1
        }
        log.info(s"after ${innerHashSet.size}")
      }
    }
    log.info(s"Found totally ${innerHashSet.size} in ${ExecutorUtils.getHostName}")
    val time0 = System.nanoTime()
    //Build outer oids
    val outerHashSet = new OpenHashSet[Long]
    for (shuffle <- lists){
      log.info(s"Extract outer vertices from ${shuffle}")
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
  def buildEdataArray(csr : GraphXCSR[Long], defaultED : ED) : Array[ED] = {
    if (defaultED != null && lists(0).getArrays._3 == null){
      val edgeNum = lists.map(_.totalSize()).sum
      Array.fill[ED](edgeNum.toInt)(defaultED)
    }
    else if (defaultED == null && lists(0).getArrays._3 != null){
      val allArrays = lists.flatMap(_.getArrays._3).toArray
      val len = allArrays.map(_.length).sum
      val edataArray = new Array[ED](len)
      //got all edge data array
      var ind = 0

      for (arr <- allArrays){
        var i = 0
        val t = arr.length
        while (i < t){
          edataArray(ind) =  arr(i)
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

  // no edata building is needed, we only persist edata to c++ when we run pregel
  def buildCSR(globalVMID : Long): (GraphXVertexMap[Long,Long], GraphXCSR[Long]) = {
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
      log.info(s"Processing ${shuffle}")
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
    graphxCSRBuilder.loadEdges(srcOids,dstOids,graphxVertexMap)
    val graphxCSR = graphxCSRBuilder.seal(client).get()
    (graphxVertexMap,graphxCSR)
  }

  //call this to delete c++ ptr and release memory of arrow builders
  def clearBuilders() : Unit = {
    lists = null
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
