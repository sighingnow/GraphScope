package com.alibaba.graphscope.graphx.rdd.impl

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.graph.GraphStructure
import com.alibaba.graphscope.graphx.rdd.RoutingTable
import com.alibaba.graphscope.graphx.store.{AbstractDataStore, AbstractInHeapDataStore, DataStore, InHeapVertexDataStore}
import com.alibaba.graphscope.graphx.utils.{BitSetWithOffset, GrapeUtils, IdParser, PrimitiveVector}
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import org.apache.spark.Partition
import org.apache.spark.graphx.{EdgeDirection, PartitionID, VertexId}
import org.apache.spark.internal.Logging

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class VertexDataMessage[VD: ClassTag](val dstPid : Int, val gids : Array[Long], val newData : Array[VD]) extends Serializable{

}
class GrapeVertexPartition[VD : ClassTag](val pid : Int,
                                          val startLid : Int,
                                          val endLid : Int,
                                          val localId : Int,
                                          val localNum : Int,
                                          val siblingPid : Array[Int],
                                          val graphStructure: GraphStructure,
                                          val vertexData: InHeapVertexDataStore[VD],
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
    vertexData.getData(lid)
  }

  def iterator : Iterator[(VertexId,VD)] = {
    new Iterator[(VertexId,VD)]{
      var lid = bitSet.nextSetBit(startLid)
      override def hasNext: Boolean = {
        lid >= 0 && lid < endLid
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

  def createNewValues[VD2: ClassTag] :InHeapVertexDataStore[VD2] = {
//      vertexData.synchronized {
//        if (!AbstractDataStore.map.contains(vertexData)) {
//          log.info(s"part ${pid} localid ${localId} found no mapping of ${vertexData} is empty, creating")
//          val newValues = vertexData.mapToNew[VD2]
//          AbstractDataStore.set(vertexData, newValues, localNum)
//          AbstractDataStore.get(vertexData)
//        }
//        else {
//          val res = AbstractDataStore.get(vertexData)
//          log.info(s"part ${pid} localid ${localId} found mapping of ${vertexData} no-empty, get res ${res}")
//          res
//        }
//    }.asInstanceOf[InHeapVertexDataStore[VD2]]
    if (localId == 0){
      val newValues = vertexData.mapToNew[VD2].asInstanceOf[InHeapVertexDataStore[VD2]]
      log.info(s"pid ${pid} create new values for part ${siblingPid.mkString(",")}, new class ${GrapeUtils.getRuntimeClass[VD2].getSimpleName}")
      for (dstPid <- siblingPid){
        InHeapVertexDataStore.enqueue(dstPid, newValues)
      }
    }
    InHeapVertexDataStore.dequeue(pid).asInstanceOf[InHeapVertexDataStore[VD2]]
  }

  def collectNbrIds(edgeDirection: EdgeDirection) : GrapeVertexPartition[Array[VertexId]] = {
    var lid = bitSet.nextSetBit(startLid)
    //    val newValues = vertexData.getOrCreate[Array[Long]](pid).asInstanceOf[InHeapVertexDataStore[Array[Long]]]
    val newValues = createNewValues[Array[VertexId]]

    while (lid >= 0 && lid < endLid){
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
        val gids = new PrimitiveVector[Long](endLid - startLid)
        val newData = new PrimitiveVector[VD](endLid - startLid)
        var j = lids.nextSetBit(startLid)
        while (j >= 0 && j < endLid){
          gids.+=(idParser.generateGlobalId(curFid, j))
          newData.+=(getData(j))
          require(getData(j) != null, s"generating vd msg encounter null at pos ${j}")
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
      for (i <- 0 until ivnum){
        require(vertexData.getData(i) != null, s"before updating, pos ${i} is null, ivnum ${ivnum}")
      }
      val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
      val threads = new ArrayBuffer[Thread]
      var tid = 0
      val queue = new ArrayBlockingQueue[(Array[Long],Array[VD])](10240)
      while (vertexDataMessage.hasNext){
        val tuple = vertexDataMessage.next()
        require(tuple._1 == pid)
        queue.offer((tuple._2.gids,tuple._2.newData))
      }
      log.info(s"totally ${queue.size()} received vd msg")
      val atomicCnt = new AtomicInteger(0)
      while (tid < localNum){
        val newThread= new Thread(){
          override def run(): Unit = {
            while (queue.size() > 0) {
              val res = queue.poll()
              val outerDatas = res._2
              val outerGids = res._1
              atomicCnt.getAndAdd(outerGids.length)
              var i = 0
              while (i < outerGids.length) {
                require(graphStructure.outerVertexGid2Vertex(outerGids(i), vertex))
                require(outerDatas(i) != null, s"received null msg in ${res}, pos ${i}")
                vertexData.setData(vertex.GetValue.toInt, outerDatas(i))
                i += 1
              }
            }
          }
        }
        newThread.start()
        threads.+=(newThread)
        tid += 1
      }
      for (i <- 0 until localNum){
        threads(i).join()
      }
      val time1 = System.nanoTime()
      log.info(s"[Perf: ] updating outer vertex data cost ${(time1 - time0) / 1000000}ms, size ${atomicCnt.get()}, ovnum ${graphStructure.getOuterVertexSize}")
    }
    else {
//      log.info(s"[Perf]: part ${pid} receives no outer vertex data, startLid ${startLid}")
    }
    this
  }

  def map[VD2: ClassTag](f: (VertexId, VD) => VD2): GrapeVertexPartition[VD2] = {
    // Construct a view of the map transformation
    val time0 = System.nanoTime()
//    val newValues = vertexData.getOrCreate[VD2](pid).asInstanceOf[InHeapVertexDataStore[VD2]]
    val newValues = createNewValues[VD2]
    var i = bitSet.nextSetBit(startLid)
    while (i >= 0 && i < endLid) {
      newValues.setData(i,f(graphStructure.getId(i), getData(i)))
      i = bitSet.nextSetBit(i + 1)
    }
    val time1 = System.nanoTime()
    log.info(s"part ${pid} from ${startLid} to ${endLid} map vertex partition from ${GrapeUtils.getRuntimeClass[VD].getSimpleName} to ${GrapeUtils.getRuntimeClass[VD2].getSimpleName}, active ${bitSet.cardinality()} cost ${(time1 - time0) / 1000000} ms")
//    for (i <- 0 until vertexData.ivnum){
//      require(newValues.getData(i) != null)
//    }
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
//    val newValues = vertexData.getOrCreate[VD2](pid).asInstanceOf[InHeapVertexDataStore[VD2]]
    val newValues = createNewValues[VD2]

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
      while (i >= 0 && i < endLid) {
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
      log.info(s"${GrapeUtils.getRuntimeClass[VD3].getSimpleName}")
//      val newValues = vertexData.getOrCreate[VD3](pid).asInstanceOf[InHeapVertexDataStore[VD3]]
      val newValues = createNewValues[VD3]
      var i = this.bitSet.nextSetBit(startLid)
      while (i >= 0 && i < endLid) {
        val otherV: Option[VD2] = if (other.bitSet.get(i)) Some(other.getData(i)) else None
        val t = f(this.graphStructure.getId(i), this.getData(i), otherV)
        if (t == null) {
          log.info(s"when join ${this} and ${other} at pos ${i} result is null")
          throw new IllegalStateException("null......")
        }
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
//    val newValues = vertexData.getOrCreate[VD2](pid).asInstanceOf[InHeapVertexDataStore[VD2]]
    val newValues = createNewValues[VD2]
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
//      val newView = vertexData.getOrCreate[VD2](pid).asInstanceOf[InHeapVertexDataStore[VD2]]
      val newView = createNewValues[VD2]
      var i = newMask.nextSetBit(startLid)
      while (i >= 0 && i < endLid) {
        newView.setData(i, f(this.graphStructure.getId(i), this.getData(i), other.getData(i)))
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewValues(newView).withMask(newMask)
    }
  }

  def withNewValues[VD2 : ClassTag](vds: InHeapVertexDataStore[VD2]) : GrapeVertexPartition[VD2] = {
    new GrapeVertexPartition[VD2](pid, startLid,endLid,localId, localNum,siblingPid, graphStructure, vds, client, routingTable, bitSet)
  }

  def withMask(newMask: BitSetWithOffset): GrapeVertexPartition[VD] ={
    new GrapeVertexPartition[VD](pid, startLid,endLid,localId, localNum,siblingPid,graphStructure, vertexData, client,routingTable, newMask)
  }

  override def toString: String = "GrapeVertexPartition{" + "pid=" + pid + ",startLid=" + startLid + ", endLid=" + endLid + ",active=" + bitSet.cardinality() + '}'

  override def index: PartitionID = pid
}

object GrapeVertexPartition extends Logging{
//  val pid2OuterVertexStore : mutable.HashMap[Int,InHeapVertexDataStore[_]] = new mutable.HashMap[Int,InHeapVertexDataStore[_]]
  val pid2VertexStore : mutable.HashMap[Int,InHeapVertexDataStore[_]] = new mutable.HashMap[Int,InHeapVertexDataStore[_]]

  def setVertexStore(pid : Int, store:InHeapVertexDataStore[_]) : Unit = {
    require(!pid2VertexStore.contains(pid))
    pid2VertexStore(pid) = store
    log.info(s"storing part ${pid}'s inner vd store ${store.toString}'")
  }

  def buildPrimitiveVertexPartition[VD: ClassTag](value : VD, pid : Int, startLid : Long, endLid : Long,localId: Int, localNum : Int, siblingPids : Array[Int],client : VineyardClient, graphStructure: GraphStructure,routingTable: RoutingTable) : GrapeVertexPartition[VD] = {
//    require(graphStructure.getVertexSize == fragVnums, s"csr inner vertex should equal to vmap ${graphStructure.getInnerVertexSize}, ${fragVnums}")
    //copy to heap
//    val newVertexData = new InHeapVertexDataStore[VD](offset = startLid.toInt, (endLid - startLid).toInt,client)
    val vertexStore = pid2VertexStore(pid).asInstanceOf[InHeapVertexDataStore[VD]]
//    val innerStoreView = new VertexDataStoreView[VD](vertexStore, startLid.toInt, endLid.toInt)
    var i = startLid.toInt
    val limit = endLid
    while (i < limit){
      vertexStore.setData(i,value)
      i += 1
    }

    new GrapeVertexPartition[VD](pid, startLid.toInt, endLid.toInt,localId, localNum,siblingPids, graphStructure, vertexStore, client,routingTable)
  }
}
