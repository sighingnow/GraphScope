package com.alibaba.graphscope.graphx.rdd.impl

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.graphx.graph.GraphStructure
import com.alibaba.graphscope.graphx.graph.impl.GraphXGraphStructure
import com.alibaba.graphscope.graphx.rdd.RoutingTable
import com.alibaba.graphscope.graphx.store.{InHeapVertexDataStore, VertexDataStore}
import com.alibaba.graphscope.graphx.utils.{IdParser, ScalaFFIFactory}
import com.alibaba.graphscope.graphx.{VertexDataBuilder, VineyardClient}
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import org.apache.spark.Partition
import org.apache.spark.graphx.{EdgeDirection, PartitionID, VertexId}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class VertexDataMessage[VD: ClassTag](val dstPid : Int, val gids : Array[Long], val newData : Array[VD]) extends Serializable{

}
class GrapeVertexPartition[VD : ClassTag](val pid : Int,
                                          val graphStructure: GraphStructure,
                                          val vertexData: VertexDataStore[VD],
                                          val client : VineyardClient,
                                          val routingTable: RoutingTable,
                                          var bitSet: BitSet = null) extends Logging with Partition {
  val startLid = 0
  val endLid = graphStructure.getInnerVertexSize
  val partVnum : Long = endLid - startLid
  val fragVnum : Int = graphStructure.getVertexSize.toInt
  val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
  if (bitSet ==null){
    bitSet = new BitSet(partVnum.toInt)
    bitSet.setUntil(endLid.toInt)
  }

  def getData(lid : Long) : VD = {
    vertexData.getData(lid)
  }

  def iterator : Iterator[(VertexId,VD)] = {
    new Iterator[(VertexId,VD)]{
      var lid = bitSet.nextSetBit(0)
      override def hasNext: Boolean = {
        lid >= 0
      }

      override def next(): (VertexId, VD) = {
        val res = (graphStructure.innerVertexLid2Oid(lid), getData(lid))
        lid  = bitSet.nextSetBit(lid + 1)
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
    var lid = bitSet.nextSetBit(0)
    val newValues = new Array[Array[VertexId]](vertexData.size.toInt)
    while (lid >= 0){
      newValues(lid) = getNbrIds(lid, edgeDirection)
      lid = bitSet.nextSetBit(lid + 1);
    }
    this.withNewValues(vertexData.withNewValues(newValues))
  }

  def generateVertexDataMessage : Iterator[(PartitionID, VertexDataMessage[VD])] = {
    val res = new ArrayBuffer[(PartitionID,VertexDataMessage[VD])]()
    if (graphStructure.isInstanceOf[GraphXGraphStructure]){
      val casted = graphStructure.asInstanceOf[GraphXGraphStructure]
      log.info(s"vm addr ${casted.vm.getAddress}")
    }
    val curFid = graphStructure.fid()
    val idParser = new IdParser(graphStructure.fnum())
    for (i <- 0 until(routingTable.numPartitions)){
      val lids = routingTable.get(i)
      if (lids != null){
        val gids = new Array[Long](lids.length)
        val newData = new Array[VD](lids.length)
        var j = 0
        while (j < lids.length){
          gids(j) = idParser.generateGlobalId(curFid, lids(j))
//          require(lids(j) < endLid)
          newData(j) = getData(lids(j))
//          log.info(s"send outer vd ${newData(j)} to gid ${gids(j)}")
          j += 1
        }
        val msg = new VertexDataMessage[VD](i, gids,newData)
        res.+=((i, msg))
        log.info(s"Partitoin ${pid} send vertex data to ${i}, size ${lids.length}")
      }
    }
    res.toIterator
  }

  def updateOuterVertexData(vertexDataMessage: Iterator[(PartitionID,VertexDataMessage[VD])]): GrapeVertexPartition[VD] = {
    val time0 = System.nanoTime()
    log.info("Start updating outer vertex")
    val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    while (vertexDataMessage.hasNext){
      val (dstPid, msg) = vertexDataMessage.next()
      require(dstPid == pid)
      val outerGids = msg.gids
      val outerDatas = msg.newData
      var i = 0
      while (i < outerGids.length){
        require(graphStructure.outerVertexGid2Vertex(outerGids(i), vertex))
        vertexData.setData(vertex.GetValue, outerDatas(i))
        i += 1
      }
    }
    val time1 = System.nanoTime()
    log.info(s"[Perf: ] updating outer vertex data cost ${(time1 - time0) / 1000000}ms")
    this
  }

  def map[VD2: ClassTag](f: (VertexId, VD) => VD2): GrapeVertexPartition[VD2] = {
    // Construct a view of the map transformation
    val time0 = System.nanoTime()
    val newValues = new Array[VD2](graphStructure.getVertexSize.toInt)
    var i = bitSet.nextSetBit(0)
    while (i >= 0) {
      newValues(i) =  f(graphStructure.getId(i), getData(i))
      i = bitSet.nextSetBit(i + 1)
    }
    val time1 = System.nanoTime()
    log.info(s"map vertex partition cost ${(time1 - time0) / 1000000} ms")
    this.withNewValues(vertexData.withNewValues[VD2](newValues))
  }

  def filter(pred: (VertexId, VD) => Boolean): GrapeVertexPartition[VD] = {
    //    // Allocate the array to store the results into
    val newMask = new BitSet(partVnum.toInt)

    // Iterate over the active bits in the old mask and evaluate the predicate
    var curLid = bitSet.nextSetBit(startLid)
    while (curLid >= 0 && curLid < endLid) {
//      log.info(s"check vertex lid ${curLid}(oid ${graphStructure.getId(curLid)} active vertices ${bitSet.cardinality()}")
      if (pred(graphStructure.getId(curLid), getData(curLid))){
//        log.info(s"vertex lid ${curLid}(oid ${graphStructure.getId(curLid)} matches pred")
        newMask.set(curLid)
      }
      curLid = bitSet.nextSetBit(curLid + 1)
    }
    this.withMask(newMask)
  }

  def aggregateUsingIndex[VD2: ClassTag](
                                          iter: Iterator[Product2[VertexId, VD2]],
                                          reduceFunc: (VD2, VD2) => VD2): GrapeVertexPartition[VD2] = {
    val newMask = new BitSet(partVnum.toInt)
    val newValues = new Array[VD2](fragVnum)

    iter.foreach { product =>
      val oid = product._1
      val vdata = product._2
      require(graphStructure.getVertex(oid,vertex))
      val lid = vertex.GetValue().toInt
      if (lid >= 0) {
        if (newMask.get(lid)) {
          newValues(lid) = reduceFunc(newValues(lid), vdata)
        } else { // otherwise just store the new value
          newMask.set(lid)
          newValues(lid) =  vdata
        }
      }
    }
    this.withNewValues(vertexData.withNewValues(newValues)).withMask(newMask)
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
      var i = newMask.nextSetBit(0)
      while (i >= 0 && i < partVnum) {
        if (getData(i) == other.getData(i)) {
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewValues(other.vertexData).withMask(newMask)
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
      val newValues = new Array[VD3](fragVnum)
      var i = this.bitSet.nextSetBit(0)
      while (i >= 0) {
        val otherV: Option[VD2] = if (other.bitSet.get(i)) Some(other.getData(i)) else None
        newValues(i) = f(this.graphStructure.getId(i), this.getData(i), otherV)
        i = this.bitSet.nextSetBit(i + 1)
      }
      val time1 = System.nanoTime()
      log.info(s"Left join between ${this} and ${other} cost ${(time1 - time0) / 1000000} ms")
      this.withNewValues(vertexData.withNewValues(newValues))
    }
  }

  /**
   * Similar effect as aggregateUsingIndex((a, b) => a)
   */
  def createUsingIndex[VD2: ClassTag](iter: Iterator[Product2[VertexId, VD2]])
  : GrapeVertexPartition[VD2] = {
    val newMask = new BitSet(partVnum.toInt)
    val newValues = new Array[VD2](fragVnum)
    iter.foreach { pair =>
//      val pos = self.index.getPos(pair._1)
      val vertexFound = graphStructure.getVertex(pair._1,vertex)
      if (vertexFound){
        val lid = vertex.GetValue().toInt
        newMask.set(lid)
        newValues(lid) = pair._2
      }
    }
    this.withNewValues(vertexData.withNewValues(newValues)).withMask(newMask)
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
      val newValues = new Array[VD2](fragVnum)
      var i = newMask.nextSetBit(startLid)
      while (i >= 0) {
        newValues(i) =  f(this.graphStructure.getId(i), this.getData(i), other.getData(i))
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewValues(vertexData.withNewValues(newValues)).withMask(newMask)
    }
  }

  def withNewValues[VD2 : ClassTag](vds: VertexDataStore[VD2]) : GrapeVertexPartition[VD2] = {
    new GrapeVertexPartition[VD2](pid, graphStructure, vds,client, routingTable, bitSet)
  }

  def withMask(newMask: BitSet): GrapeVertexPartition[VD] ={
    new GrapeVertexPartition[VD](pid, graphStructure, vertexData, client,routingTable, newMask)
  }

  override def toString: String = "GrapeVertexPartition{" + "pid=" + pid + ",startLid=" + startLid + ", endLid=" + endLid + ",active=" + bitSet.capacity + '}'

  override def index: PartitionID = pid
}

object GrapeVertexPartition extends Logging{
  def buildPrimitiveVertexPartition[VD: ClassTag](fragVnums : Long, value : VD, pid : Int, client : VineyardClient, graphStructure: GraphStructure, routingTable: RoutingTable) : GrapeVertexPartition[VD] = {
    require(graphStructure.getVertexSize == fragVnums, s"csr inner vertex should equal to vmap ${graphStructure.getInnerVertexSize}, ${fragVnums}")
    //copy to heap
    val newArray = new Array[VD](fragVnums.toInt)
    var i = 0
    val limit = fragVnums
    while (i < limit){
      newArray(i) = value
      i += 1
    }
    val newVertexData = new InHeapVertexDataStore[VD](newArray,client,0)
    new GrapeVertexPartition[VD](pid, graphStructure, newVertexData, client, routingTable)
  }
}
