package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.ds.PropertyNbrUnit
import com.alibaba.graphscope.graphx.{GSEdgeTriplet, GSEdgeTripletImpl, GraphXCSR, GraphXVertexMap, LocalVertexMap, ReverseGSEdgeTripletImpl, VineyardClient}
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.impl.{EdgePartition, GrapeUtils}
import org.apache.spark.graphx.impl.partition.data.{EdgeDataStore, InHeapEdataStore, VertexDataStore}
import org.apache.spark.graphx.utils.{ExecutorUtils, ScalaFFIFactory}
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, ReusableEdge, ReusableEdgeImpl, ReversedReusableEdge, VertexId}
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
                                                     val edataStore : EdgeDataStore[ED],
                                                     val client : VineyardClient,
                                                     val edgeReversed : Boolean = false,
                                                     var activeEdgeSet : BitSet = null,
                                                     var srcLids : PrimitiveArray[Long] = null) extends Logging {
  val startLid = 0
  val endLid : Long = vm.innerVertexSize()
  def partOutEdgeNum : Long = csr.getPartialOutEdgesNum(startLid, endLid)
  def partInEdgeNum : Long = csr.getPartialInEdgesNum(startLid, endLid)

  if (activeEdgeSet == null){
    activeEdgeSet = new BitSet(csr.getOEOffset(endLid).toInt) // just need to control out edges
    activeEdgeSet.setUntil(csr.getOEOffset(endLid).toInt)
  }
  val NBR_SIZE = 16L
  //to avoid the difficult to get srcLid in iterating over edges.

  initSrcLid()
  def initSrcLid() : Unit = {
    val time0 = System.nanoTime()
    if (srcLids == null){
      srcLids = PrimitiveArray.create(classOf[Long], (endLid - startLid).toInt)
      var curLid = 0
      while (curLid < endLid){
        val startNbrOffset = csr.getOEOffset(curLid)
        val endNbrOffset = csr.getOEOffset(curLid + 1)
        var j = startNbrOffset
        while (j < endNbrOffset){
          j += 1
          srcLids.set(j, curLid)
        }
        curLid += 1
      }
    }
    val time1 = System.nanoTime()
    log.info(s"[Initializing srcLids cost ]: ${(time1 - time0) / 1000000} ms")
  }

  log.info(s"Got edge partition ${this.toString}")


  def getDegreeArray(edgeDirection: EdgeDirection): PrimitiveArray[Int] = {
    if (edgeDirection.equals(EdgeDirection.In)){
      getInDegreeArray
    }
    else if (edgeDirection.equals(EdgeDirection.Out)){
      getOutDegreeArray
    }
    else{
      getInOutDegreeArray
    }
  }

  def getOutDegreeArray : PrimitiveArray[Int] = {
    val len = vm.getVertexSize.toInt
    val res = PrimitiveArray.create(classOf[Int], len)
    var i = 0L
    while (i < endLid){
      res.set(i, csr.getOutDegree(i).toInt)
      i += 1
    }
    while (i < len){
      res.set(i, 0)
      i += 1
    }
    res
  }

  def getInDegreeArray : PrimitiveArray[Int] = {
    val len = vm.getVertexSize.toInt
    val res = PrimitiveArray.create(classOf[Int], len)
    var i = 0L
    while (i < endLid){
      res.set(i, csr.getInDegree(i).toInt)
      i += 1
    }
    while (i < len){
      res.set(i, 0)
      i += 1
    }
    res
  }

  def getInOutDegreeArray : PrimitiveArray[Int] = {
    val len = vm.getVertexSize.toInt
    val res = PrimitiveArray.create(classOf[Int], len)
    var i = 0L
    while (i < endLid) {
      res.set(i, csr.getInDegree(i).toInt)
      i += 1
    }
    while (i < len) {
      res.set(i, 0)
      i += 1
    }
    res
  }

  /** Iterate over out edges only, in edges will be iterated in other partition */
  def iterator : Iterator[Edge[ED]] = {
    new Iterator[Edge[ED]]{
      val curNbr: PropertyNbrUnit[VertexId] = csr.getOEBegin(startLid)
      val beginAddr = curNbr.getAddress
      var offset : Long = activeEdgeSet.nextSetBit(0)
      val offsetLimit : Long = csr.getOEOffset(endLid)
      curNbr.setAddress(beginAddr + offset * NBR_SIZE)
      var edge: ReusableEdge[ED] = null.asInstanceOf[ReusableEdge[ED]]

      if (edgeReversed){
        edge = new ReversedReusableEdge[ED];
      }
      else {
        edge = new ReusableEdgeImpl[ED];
      }
      log.info(s"Initiate iterator on partition ${pid} ,reversed ${edgeReversed}")

      override def hasNext: Boolean = {
//        log.info(s"has next offset: ${offset}, limit ${offsetLimit}")
        if (offset >= 0 && offset < offsetLimit) true
        else false
      }

      override def next() : Edge[ED] = {
//        log.info(s"curLid ${curLid},offset ${offset}, offset limit${offsetLimit}")
        curNbr.setAddress(beginAddr + offset * NBR_SIZE)
        val curLid = srcLids.get(offset)
        val dstLid = curNbr.vid()
        val edata = edataStore.getData(curNbr.eid())
        edge.eid = curNbr.eid()
        edge.srcId = vm.getId(curLid)
        edge.dstId = vm.getId(dstLid)
        edge.attr = edata
        edge.index = offset
        offset = activeEdgeSet.nextSetBit(offset.toInt + 1)
        edge
      }
    }
  }

  def tripletIterator(vertexDataStore: VertexDataStore[VD],
                       includeSrc: Boolean = true, includeDst: Boolean = true)
  : Iterator[EdgeTriplet[VD, ED]] = new Iterator[EdgeTriplet[VD, ED]] {

    val curNbr: PropertyNbrUnit[VertexId] = csr.getOEBegin(startLid)
    val beginAddr = curNbr.getAddress
    var offset : Long = activeEdgeSet.nextSetBit(0)
    val offsetLimit : Long = csr.getOEOffset(endLid)
    curNbr.setAddress(beginAddr + offset * NBR_SIZE)

    def createTriplet : GSEdgeTriplet[VD,ED] = {
      if (!edgeReversed){
        new GSEdgeTripletImpl[VD,ED];
      }
      else {
        new ReverseGSEdgeTripletImpl[VD,ED]
      }
    }

    log.info(s"Initiate iterator on partition ${pid} ,reversed ${edgeReversed}")

    override def hasNext: Boolean = {
//      log.info(s"has next offset: ${offset}, limit ${offsetLimit}")
      if (offset >= 0 && offset < offsetLimit) true
      else false
    }

    override def next() : EdgeTriplet[VD,ED] = {
      //Find srcLid of curNbr
      val edgeTriplet = createTriplet
//      log.info(s"curLid ${curLid},offset ${offset}, offset limit${offsetLimit}")
      curNbr.setAddress(beginAddr + offset * NBR_SIZE)
      val curLid = srcLids.get(offset)
      val dstLid = curNbr.vid()
      val edata = edataStore.getData(curNbr.eid())
      edgeTriplet.eid = curNbr.eid()
      edgeTriplet.index = offset
      edgeTriplet.srcId = vm.getId(curLid)
      edgeTriplet.dstId = vm.getId(dstLid)
      edgeTriplet.attr = edata
      if (includeSrc){
        edgeTriplet.srcAttr = vertexDataStore.getData(curLid)
      }
      if (includeDst){
        edgeTriplet.dstAttr = vertexDataStore.getData(dstLid)
      }
      offset = activeEdgeSet.nextSetBit(offset.toInt + 1)
      //        curNbr.addV(NBR_SIZE)
      log.info(s"Produce edge Triplet: ${edgeTriplet}")
      edgeTriplet
    }
  }

  def filter(
              epred: EdgeTriplet[VD, ED] => Boolean,
              vpred: (VertexId, VD) => Boolean,
              vertexDataStore: VertexDataStore[VD]): GrapeEdgePartition[VD, ED] = {
    //First invalided all invalid edges from invalid vertices.
    val tripletIter = tripletIterator(vertexDataStore).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    val newActiveEdges = new BitSet(csr.getOEOffset(endLid).toInt)
    newActiveEdges.union(activeEdgeSet)
    while (tripletIter.hasNext){
      val triplet = tripletIter.next()
      if (!vpred(triplet.srcId,triplet.srcAttr) || !vpred(triplet.dstId, triplet.dstAttr) || !epred(triplet)){
        activeEdgeSet.unset(triplet.index.toInt)
        log.info(s"Inactive edge ${triplet}")
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
    val newEdata = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED], edataStore.size.toInt).asInstanceOf[PrimitiveArray[ED]]
    while (iter.hasNext){
      val edge = iter.next()
      val curIndex = edge.index
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
          newEdata.set(prevEdgeInd, attrSum)
          log.info(s"end of acculating edge ${curSrcId}, ${curDstId}, ${attrSum}")
        }
        curSrcId = edge.srcId
        curDstId = edge.dstId
        prevEdgeInd = curIndex
        attrSum = edge.attr
      }
      flag = true
    }
    new GrapeEdgePartition[VD,ED](pid, csr, vm, new InHeapEdataStore[ED](newEdata), client, edgeReversed, newMask, srcLids)
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[VD, ED2] = {
    val newData = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED2], edataStore.size.toInt).asInstanceOf[PrimitiveArray[ED2]]
    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    var ind = 0;
    while (iter.hasNext){
      val edge = iter.next()
      newData.set(edge.eid, f(edge))
      ind += 1
    }
    this.withNewEdata(newData)
  }

  def map[ED2: ClassTag](iter: Iterator[ED2]): GrapeEdgePartition[VD, ED2] = {
    val newData = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED2], edataStore.size.toInt).asInstanceOf[PrimitiveArray[ED2]]
    var ind = activeEdgeSet.nextSetBit(0)
    val curNbr: PropertyNbrUnit[VertexId] = csr.getOEBegin(0)
    val beginAddr = curNbr.getAddress
    while (iter.hasNext) {
      val curAddr = beginAddr + ind * NBR_SIZE
      curNbr.setAddress(curAddr)
      val eid = curNbr.eid()
      newData.set(eid, iter.next())
      require(ind != -1, s"mapping edges: received edge iterator length neq to cur active edges ${activeEdgeSet.cardinality()}")
      ind = activeEdgeSet.nextSetBit(ind + 1)
    }
    require(ind == -1, s"after map new edata, ind ${ind}, expect edata size ${activeEdgeSet.cardinality()}")
    this.withNewEdata(newData)
  }

  def reverse: GrapeEdgePartition[VD, ED] = {
    new GrapeEdgePartition[VD,ED](pid, csr, vm, edataStore, client,!edgeReversed, activeEdgeSet,srcLids)
  }

  def withNewEdata[ED2: ClassTag](newEdata : PrimitiveArray[ED2]): GrapeEdgePartition[VD, ED2] = {
    new GrapeEdgePartition[VD,ED2](pid, csr.asInstanceOf[GraphXCSR[Long,ED2]],vm, new InHeapEdataStore[ED2](newEdata) ,client,edgeReversed,activeEdgeSet,srcLids)
  }

  def withNewMask(newActiveSet: BitSet) : GrapeEdgePartition[VD,ED] = {
    new GrapeEdgePartition[VD,ED](pid, csr, vm, edataStore, client, edgeReversed, newActiveSet,srcLids)
  }

  /**  currently we only support inner join with same vertex map*/
  def innerJoin[ED2: ClassTag, ED3: ClassTag]
  (other: GrapeEdgePartition[_, ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgePartition[VD, ED3] = {
    if (this.vm != other.vm){
      throw new IllegalStateException("Currently we only support inner join with same index")
    }
    val newMask = this.activeEdgeSet & other.activeEdgeSet
    log.info(s"Inner join edgePartition 0 has ${this.activeEdgeSet.cardinality()} actives edges, the other has ${other.activeEdgeSet} active edges")
    log.info(s"after join ${newMask.cardinality()} active edges")
    val newEdata = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED3], edataStore.size.toInt).asInstanceOf[PrimitiveArray[ED3]]
    val oldIter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    while (oldIter.hasNext){
      val oldEdge = oldIter.next()
      val oldIndex = oldEdge.index.toInt
      if (newMask.get(oldIndex)){
        newEdata.set(oldIndex, f(oldEdge.srcId, oldEdge.dstId, oldEdge.attr, other.edataStore.getData(oldIndex)))
      }
    }

    new GrapeEdgePartition[VD,ED3](pid, csr.asInstanceOf[GraphXCSR[Long,ED3]], vm, new InHeapEdataStore[ED3](newEdata), client,edgeReversed, newMask,srcLids)
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
