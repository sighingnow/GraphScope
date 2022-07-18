package com.alibaba.graphscope.graphx.rdd.impl

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.graph.GraphStructure
import com.alibaba.graphscope.graphx.graph.impl.GraphXGraphStructure
import com.alibaba.graphscope.graphx.rdd.RoutingTable
import com.alibaba.graphscope.graphx.store.{InHeapVertexDataStore, VertexDataStore, VertexDataStoreView}
import com.alibaba.graphscope.graphx.utils.{BitSetWithOffset, GrapeUtils, IdParser, PrimitiveVector}
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import org.apache.spark.Partition
import org.apache.spark.graphx.{EdgeDirection, PartitionID, VertexId}
import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class VertexDataMessage[VD: ClassTag](val dstPid : Int, val gids : Array[Long], val newData : Array[VD]) extends Serializable{

}
class GrapeVertexPartition[VD : ClassTag](val pid : Int,
                                          val startLid : Int,
                                          val endLid : Int,
                                          val graphStructure: GraphStructure,
                                          val innerVertexData: VertexDataStore[VD],
                                          val outerVertexData : VertexDataStore[VD],
                                          val client : VineyardClient,
                                          val routingTable: RoutingTable,
                                          var bitSet: BitSetWithOffset = null) extends Logging with Partition {

  val partVnum : Long = endLid - startLid
  val fragVnum : Int = graphStructure.getVertexSize.toInt
  val ivnum = graphStructure.getInnerVertexSize.toInt
  val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
  if (bitSet ==null){
    bitSet = new BitSetWithOffset(startBit = startLid,endBit = endLid)
    bitSet.setRange(startLid,endLid)
  }

  def getData(lid : Int) : VD = {
    if (lid < ivnum) innerVertexData.getData(lid)
    else outerVertexData.getData(lid)
  }

  def iterator : Iterator[(VertexId,VD)] = {
    new Iterator[(VertexId,VD)]{
      var lid = bitSet.nextSetBit(startLid)
      override def hasNext: Boolean = {
        lid >= 0
      }

      override def next(): (VertexId, VD) = {
        val res = (graphStructure.innerVertexLid2Oid(lid), getData(lid))
        lid = bitSet.nextSetBit(lid + 1)
        res
      }
    }
  }

  def getNbrIds(lid: Int, edgeDirection: EdgeDirection) : Array[Long] = {
    if (edgeDirection.equals(EdgeDirection.In)){
      graphStructure.getInNbrIds(lid)
    }
    else if (edgeDirection.equals(EdgeDirection.Out)){
      graphStructure.getOutNbrIds(lid)
    }
    else if (edgeDirection.equals(EdgeDirection.Either)){
      graphStructure.getInOutNbrIds(lid)
    }
    else {
      throw new IllegalStateException(s"Not supported direction ${edgeDirection.toString()}")
    }
  }

  def collectNbrIds(edgeDirection: EdgeDirection) : GrapeVertexPartition[Array[VertexId]] = {
    var lid = bitSet.nextSetBit(startLid)
    val newValues = innerVertexData.getOrCreate[Array[Long]]
    while (lid >= 0){
      newValues.setData(lid,getNbrIds(lid, edgeDirection))
      lid = bitSet.nextSetBit(lid + 1);
    }
    this.withNewValues(newValues)
  }

  def generateVertexDataMessage : Iterator[(PartitionID, VertexDataMessage[VD])] = {
    val res = new ArrayBuffer[(PartitionID,VertexDataMessage[VD])]()
    val curFid = graphStructure.fid()
    val idParser = new IdParser(graphStructure.fnum())
    for (i <- 0 until(routingTable.numPartitions)){
      val lids = routingTable.get(i)
      if (lids != null){
        val gids = new PrimitiveVector[Long]
        val newData = new PrimitiveVector[VD]
        var j = lids.nextSetBit(startLid)
        while (j >= 0 && j < endLid){
          gids.+=(idParser.generateGlobalId(curFid, j))
          newData.+=(getData(j))
          j = lids.nextSetBit(j + 1)
        }
        val msg = new VertexDataMessage[VD](i, gids.trim().array,newData.trim().array)
        res.+=((i, msg))
//        log.info(s"Partitoin ${pid} send vertex data to ${i}, size ${msg.gids.length}")
      }
    }
    res.toIterator
  }

  def updateOuterVertexData(vertexDataMessage: Iterator[(PartitionID,VertexDataMessage[VD])]): GrapeVertexPartition[VD] = {
    val time0 = System.nanoTime()
    log.info(s"Start updating outer vertex on part ${pid}")
    if (vertexDataMessage.hasNext) {
      val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
      while (vertexDataMessage.hasNext) {
        val (dstPid, msg) = vertexDataMessage.next()
        require(dstPid == pid)
        val outerGids = msg.gids
        val outerDatas = msg.newData
        var i = 0
        while (i < outerGids.length) {
          require(graphStructure.outerVertexGid2Vertex(outerGids(i), vertex))
//          require(vertex.GetValue() >= graphStructure.getInnerVertexSize)
          outerVertexData.setData(vertex.GetValue.toInt, outerDatas(i))
          i += 1
        }
      }
      val time1 = System.nanoTime()
//      log.info(s"[Perf: ] updating outer vertex data cost ${(time1 - time0) / 1000000}ms, size ${}")
    }
    else {
//      log.info(s"[Perf]: part ${pid} receives no outer vertex data, startLid ${startLid}")
    }
    this
  }

  def map[VD2: ClassTag](f: (VertexId, VD) => VD2): GrapeVertexPartition[VD2] = {
    // Construct a view of the map transformation
    val time0 = System.nanoTime()
    val newValues = innerVertexData.getOrCreate[VD2]
    var i = bitSet.nextSetBit(startLid)
    while (i >= 0) {
      newValues.setData(i,f(graphStructure.getId(i), getData(i)))
      i = bitSet.nextSetBit(i + 1)
    }
    val time1 = System.nanoTime()
    log.info(s"map vertex partition cost ${(time1 - time0) / 1000000} ms")
    this.withNewValues(newValues)
  }

  def filter(pred: (VertexId, VD) => Boolean): GrapeVertexPartition[VD] = {
    //    // Allocate the array to store the results into
    val newMask = new BitSetWithOffset(startLid,endLid)

    // Iterate over the active bits in the old mask and evaluate the predicate
    var curLid = bitSet.nextSetBit(startLid)
    while (curLid >= 0 && curLid < endLid) {
      if (pred(graphStructure.getId(curLid), getData(curLid))){
        newMask.set(curLid)
      }
      curLid = bitSet.nextSetBit(curLid + 1)
    }
    this.withMask(newMask)
  }

  def aggregateUsingIndex[VD2: ClassTag](
                                          iter: Iterator[Product2[VertexId, VD2]],
                                          reduceFunc: (VD2, VD2) => VD2): GrapeVertexPartition[VD2] = {
    val newMask = new BitSetWithOffset(startLid,endLid)
    val newValues = innerVertexData.getOrCreate[VD2]

    iter.foreach { product =>
      val oid = product._1
      val vdata = product._2
      require(graphStructure.getVertex(oid,vertex))
      val lid = vertex.GetValue().toInt
      if (lid >= 0) {
        if (newMask.get(lid)) {
          newValues.setData(lid,reduceFunc(newValues.getData(lid), vdata))
        } else { // otherwise just store the new value
          newMask.set(lid)
          newValues.setData(lid,vdata)
        }
      }
    }
    this.withNewValues(newValues).withMask(newMask)
  }

  /** Hides the VertexId's that are the same between `this` and `other`. */
  def minus(other: GrapeVertexPartition[VD]): GrapeVertexPartition[VD] = {
    if (this.graphStructure != other.graphStructure) {
      logWarning("Minus operations on two VertexPartitions with different indexes is slow.")
      minus(createUsingIndex(other.iterator))
    } else {
      this.withMask(this.bitSet.andNot(other.bitSet))
    }
  }

  def diff(other: GrapeVertexPartition[VD]): GrapeVertexPartition[VD] = {
    if (this.graphStructure != this.graphStructure) {
      logWarning("Diffing two VertexPartitions with different indexes is slow.")
      diff(createUsingIndex(other.iterator))
    } else {
      val newMask = this.bitSet & other.bitSet
      var i = newMask.nextSetBit(startLid)
      while (i >= 0) {
        if (getData(i) == other.getData(i)) {
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewValues(other.innerVertexData).withMask(newMask)
    }
  }

  def leftJoin[VD2: ClassTag, VD3: ClassTag]
  (other: GrapeVertexPartition[VD2])
  (f: (VertexId, VD, Option[VD2]) => VD3): GrapeVertexPartition[VD3] = {
    if (this.graphStructure != other.graphStructure){
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {
      /** for vertex not represented in other, we use original vertex */
      val time0 = System.nanoTime()
      log.info(s"${GrapeUtils.getRuntimeClass[VD3].getSimpleName}")
      val newValues = innerVertexData.getOrCreate[VD3]
      var i = this.bitSet.nextSetBit(startLid)
      while (i >= 0) {
        val otherV: Option[VD2] = if (other.bitSet.get(i)) Some(other.getData(i)) else None
        val t = f(this.graphStructure.getId(i), this.getData(i), otherV)
        newValues.setData(i, t)
        i = this.bitSet.nextSetBit(i + 1)
      }
      val time1 = System.nanoTime()
      log.info(s"Left join between ${this} and ${other} cost ${(time1 - time0) / 1000000} ms")
      this.withNewValues(newValues)
    }
  }

  /**
   * Similar effect as aggregateUsingIndex((a, b) => a)
   */
  def createUsingIndex[VD2: ClassTag](iter: Iterator[Product2[VertexId, VD2]])
  : GrapeVertexPartition[VD2] = {
    val newMask = new BitSetWithOffset(startLid,endLid)
//    val newValues = innerVertexData.create[VD2]
    val newValues = innerVertexData.getOrCreate[VD2]
    iter.foreach { pair =>
//      val pos = self.index.getPos(pair._1)
      val vertexFound = graphStructure.getVertex(pair._1,vertex)
      if (vertexFound){
        val lid = vertex.GetValue().toInt
        newMask.set(lid)
        newValues.setData(lid,pair._2)
      }
    }
    this.withNewValues(newValues).withMask(newMask)
  }

  /** Inner join another VertexPartition. */
  def innerJoin[U: ClassTag, VD2: ClassTag]
  (other: GrapeVertexPartition[U])
  (f: (VertexId, VD, U) => VD2): GrapeVertexPartition[VD2] = {
    if (this.graphStructure != other.graphStructure){
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      innerJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newMask = this.bitSet & other.bitSet
//      val newValues = innerVertexData.create[VD2]
      val newView = innerVertexData.getOrCreate[VD2]
      var i = newMask.nextSetBit(startLid)
      while (i >= 0) {
        newView.setData(i, f(this.graphStructure.getId(i), this.getData(i), other.getData(i)))
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewValues(newView).withMask(newMask)
    }
  }

  def withNewValues[VD2 : ClassTag](vds: VertexDataStore[VD2]) : GrapeVertexPartition[VD2] = {
    new GrapeVertexPartition[VD2](pid, startLid,endLid,graphStructure, vds, outerVertexData.getOrCreate[VD2], client, routingTable, bitSet)
  }

  def withMask(newMask: BitSetWithOffset): GrapeVertexPartition[VD] ={
    new GrapeVertexPartition[VD](pid, startLid,endLid, graphStructure, innerVertexData,outerVertexData, client,routingTable, newMask)
  }

  override def toString: String = "GrapeVertexPartition{" + "pid=" + pid + ",startLid=" + startLid + ", endLid=" + endLid + ",active=" + bitSet.cardinality() + '}'

  override def index: PartitionID = pid
}

object GrapeVertexPartition extends Logging{
  val pid2OuterVertexStore : mutable.HashMap[Int,InHeapVertexDataStore[_]] = new mutable.HashMap[Int,InHeapVertexDataStore[_]]
  val pid2InnerVertexStore : mutable.HashMap[Int,InHeapVertexDataStore[_]] = new mutable.HashMap[Int,InHeapVertexDataStore[_]]

  def setOuterVertexStore(pid : Int, store:InHeapVertexDataStore[_]) : Unit = {
    require(!pid2OuterVertexStore.contains(pid))
    pid2OuterVertexStore(pid) = store
    log.info(s"storing part ${pid}'s outer vd store ${store.toString}'")
  }

  def setInnerVertexStore(pid : Int, store:InHeapVertexDataStore[_]) : Unit = {
    require(!pid2InnerVertexStore.contains(pid))
    pid2InnerVertexStore(pid) = store
    log.info(s"storing part ${pid}'s inner vd store ${store.toString}'")
  }

  def buildPrimitiveVertexPartition[VD: ClassTag](value : VD, pid : Int, startLid : Long, endLid : Long, client : VineyardClient, graphStructure: GraphStructure,routingTable: RoutingTable) : GrapeVertexPartition[VD] = {
//    require(graphStructure.getVertexSize == fragVnums, s"csr inner vertex should equal to vmap ${graphStructure.getInnerVertexSize}, ${fragVnums}")
    //copy to heap
//    val newVertexData = new InHeapVertexDataStore[VD](offset = startLid.toInt, (endLid - startLid).toInt,client)
    val innerStore = pid2InnerVertexStore(pid).asInstanceOf[InHeapVertexDataStore[VD]]
    val innerStoreView = new VertexDataStoreView[VD](innerStore, startLid.toInt, endLid.toInt)
    var i = startLid.toInt
    val limit = endLid
    while (i < limit){
      innerStoreView.setData(i,value)
      i += 1
    }

    require(pid2OuterVertexStore(pid) != null, s"outer vd store for part ${pid} is null")
    new GrapeVertexPartition[VD](pid, startLid.toInt, endLid.toInt, graphStructure, innerStoreView, pid2OuterVertexStore(pid).asInstanceOf[InHeapVertexDataStore[VD]], client,routingTable)
  }
}
