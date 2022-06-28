package org.apache.spark.graphx.impl.grape

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.graphx._
import com.alibaba.graphscope.graphx.graph.{GSEdgeTriplet, GraphStructure, ReusableEdge}
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.impl.partition.EdgeShuffleReceived
import org.apache.spark.graphx.impl.partition.data.VertexDataStore
import org.apache.spark.graphx.utils.{ExecutorUtils, ScalaFFIFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * [startLid, endLid), endLid is exclusive
 */
class GrapeEdgePartition[VD: ClassTag, ED: ClassTag](val pid : Int,
                                                     val graphStructure: GraphStructure,
                                                     val client : VineyardClient,
                                                     var edatas : PrimitiveArray[ED],
                                                     val edgeReversed : Boolean = false,
                                                     var activeEdgeSet : BitSet = null) extends Logging {
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


  def getDegreeArray(edgeDirection: EdgeDirection): PrimitiveArray[Int] = {
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
                       includeSrc: Boolean = true, includeDst: Boolean = true)
  : Iterator[EdgeTriplet[VD, ED]] = graphStructure.tripletIterator(vertexDataStore,edatas,activeEdgeSet,includeSrc,includeDst,edgeReversed)

  def filter(
              epred: EdgeTriplet[VD, ED] => Boolean,
              vpred: (VertexId, VD) => Boolean,
              vertexDataStore: VertexDataStore[VD]): GrapeEdgePartition[VD, ED] = {
    //First invalided all invalid edges from invalid vertices.
    val tripletIter = tripletIterator(vertexDataStore).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    val newActiveEdges = new BitSet(partOutEdgeNum.toInt)
    newActiveEdges.union(activeEdgeSet)
    while (tripletIter.hasNext){
      val triplet = tripletIter.next()
      if (!vpred(triplet.srcId,triplet.srcAttr) || !vpred(triplet.dstId, triplet.dstAttr) || !epred(triplet)){
        newActiveEdges.unset(triplet.offset.toInt)
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
    val newEdata = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED], allEdgesNum.toInt).asInstanceOf[PrimitiveArray[ED]]
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
    new GrapeEdgePartition[VD,ED](pid, graphStructure, client,newEdata, edgeReversed, newMask)
  }

  def map[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgePartition[VD, ED2] = {
    val newData = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED2], allEdgesNum.toInt).asInstanceOf[PrimitiveArray[ED2]]
    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    var ind = 0;
    while (iter.hasNext){
      val edge = iter.next()
      newData.set(edge.eid, f(edge))
      ind += 1
    }
    this.withNewEdata(newData)
  }

  def map[ED2: ClassTag](f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): GrapeEdgePartition[VD, ED2] = {
    val newData = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED2], allEdgesNum.toInt).asInstanceOf[PrimitiveArray[ED2]]
    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    val resultEdata = f(pid, iter)
    val eids = graphStructure.getEids
    var ind = activeEdgeSet.nextSetBit(0);
    while (ind >= 0 && resultEdata.hasNext){
      val eid = eids.get(ind)
      newData.set(eid, resultEdata.next())
      ind = activeEdgeSet.nextSetBit(ind + 1);
    }
    if (resultEdata.hasNext || ind >= 0){
      throw new IllegalStateException(s"impossible, two iterator should end at the same time ${ind}, ${resultEdata.hasNext}")
    }
    this.withNewEdata(newData)
  }

  def mapTriplets[ED2: ClassTag](f: EdgeTriplet[VD,ED] => ED2, vertexDataStore: VertexDataStore[VD], tripletFields: TripletFields): GrapeEdgePartition[VD, ED2] = {
    val newData = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED2], allEdgesNum.toInt).asInstanceOf[PrimitiveArray[ED2]]
//    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    val iter = tripletIterator(vertexDataStore).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    var ind = 0;
    val time0 = System.nanoTime()
    while (iter.hasNext){
      val edge = iter.next()
      newData.set(edge.eid, f(edge))
      ind += 1
    }
    val time1 = System.nanoTime()
    log.info(s"[Perf:] mapping over triplets cost ${(time1 - time0)/1000000} ms")
    this.withNewEdata(newData)
  }

  def mapTriplets[ED2: ClassTag](f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2], vertexDataStore: VertexDataStore[VD], includeSrc  : Boolean = true, includeDst : Boolean = true): GrapeEdgePartition[VD, ED2] = {
    val newData = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED2], allEdgesNum.toInt).asInstanceOf[PrimitiveArray[ED2]]
    //    val iter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    val iter = tripletIterator(vertexDataStore).asInstanceOf[Iterator[GSEdgeTriplet[VD,ED]]]
    val resultEdata = f(pid, iter)
    val eids = graphStructure.getEids
    val time0 = System.nanoTime()
    var ind = activeEdgeSet.nextSetBit(0)
    while (ind >= 0 && resultEdata.hasNext){
      val eid = eids.get(ind)
      newData.set(eid, resultEdata.next())
      ind = activeEdgeSet.nextSetBit(ind + 1)
    }
    if (ind >=0 || resultEdata.hasNext){
      throw new IllegalStateException(s"impossible, two iterator should end at the same time ${ind}, ${resultEdata.hasNext}")
    }
    val time1 = System.nanoTime()
    log.info(s"[Perf:] mapping over triplets cost ${(time1 - time0)/1000000} ms")
    this.withNewEdata(newData)
  }

  def reverse: GrapeEdgePartition[VD, ED] = {
    new GrapeEdgePartition[VD,ED](pid, graphStructure, client,edatas,!edgeReversed, activeEdgeSet)
  }

  def withNewEdata[ED2: ClassTag](newEdata : PrimitiveArray[ED2]): GrapeEdgePartition[VD, ED2] = {
    log.info(s"Creating new edge partition with new edge of size ${newEdata.size()}, out edge num ${graphStructure.getOutEdgesNum}")
    new GrapeEdgePartition[VD,ED2](pid, graphStructure, client,newEdata, edgeReversed, activeEdgeSet)
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
    val newEdata = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED3],allEdgesNum.toInt).asInstanceOf[PrimitiveArray[ED3]]
    val oldIter = iterator.asInstanceOf[Iterator[ReusableEdge[ED]]]
    while (oldIter.hasNext){
      val oldEdge = oldIter.next()
      val oldIndex = oldEdge.eid.toInt
      if (newMask.get(oldIndex)){
        newEdata.set(oldIndex, f(oldEdge.srcId, oldEdge.dstId, oldEdge.attr, other.edatas.get(oldIndex))) //FIXME: edatas can be null
      }
    }

    new GrapeEdgePartition[VD,ED3](pid, graphStructure, client,newEdata,edgeReversed, newMask)
  }

  override def toString: String =  super.toString + "(pid=" + pid +
    ", start lid" + startLid + ", end lid " + endLid + ",graph structure" + graphStructure +
    ",out edges num" + partOutEdgeNum + ", in edges num" + partInEdgeNum +")"
}

class GrapeEdgePartitionBuilder[VD: ClassTag, ED: ClassTag](val numPartitions : Int,val client : VineyardClient) extends Logging{
  val srcOidBuilder: ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
  val dstOidBuilder: ArrowArrayBuilder[Long] = ScalaFFIFactory.newSignedLongArrayBuilder()
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
    val localVM = localVertexMapBuilder.seal(client).get()
    log.info(s"${ExecutorUtils.getHostName}: Finish building local vm: ${localVM.id()}, ${localVM.getInnerVerticesNum}");
    localVM
  }

  /** The received edata arrays contains both in edges and out edges, but we only need these out edges's edata array */
  def buildEdataArray(csr : GraphXCSR[Long]) : PrimitiveArray[ED] = {
    val allArrays = lists.flatMap(_.getArrays._3).toArray
    val len = allArrays.map(_.length).sum
    val edataArray = PrimitiveArray.create(GrapeUtils.getRuntimeClass[ED], len).asInstanceOf[PrimitiveArray[ED]]
    //got all edge data array
    var ind = 0

    for (arr <- allArrays){
      var i = 0
      val t = arr.length
      while (i < t){
        edataArray.set(ind, arr(i))
        i += 1
        ind += 1
      }
    }
    edataArray
  }

  // no edata building is needed, we only persist edata to c++ when we run pregel
  def buildCSR(globalVMID : Long): (GraphXVertexMap[Long,Long], GraphXCSR[Long]) = {
    val edgesNum = lists.map(shuffle => shuffle.totalSize()).sum
    log.info(s"Got totally ${lists.length}, edges ${edgesNum} in ${ExecutorUtils.getHostName}")
    srcOidBuilder.reserve(edgesNum)
    dstOidBuilder.reserve(edgesNum)
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
//          edataBuilder.unsafeAppend(attrArray(j))
          j += 1
        }
        i += 1
      }
    }
    log.info("Finish adding edges to builders")
    val graphxCSRBuilder = ScalaFFIFactory.newGraphXCSRBuilder(client)
    graphxCSRBuilder.loadEdges(srcOidBuilder,dstOidBuilder,graphxVertexMap)
    val graphxCSR = graphxCSRBuilder.seal(client).get()
    (graphxVertexMap,graphxCSR)
  }

  //call this to delete c++ ptr and release memory of arrow builders
  def clearBuilders() : Unit = {
    lists = null
  }
}
