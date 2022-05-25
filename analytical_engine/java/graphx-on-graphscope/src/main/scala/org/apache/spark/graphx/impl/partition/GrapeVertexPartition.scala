package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.graphx.{GraphXVertexMap, VertexDataBuilder, VineyardClient}
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.{PartitionID, VertexId}
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.impl.partition.data.{InHeapVertexDataStore, VertexDataStore}
import org.apache.spark.graphx.utils.ScalaFFIFactory
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class VertexDataMessage[VD: ClassTag](val dstPid : Int, val gids : Array[Long], val newData : Array[VD]) extends Serializable{

}
class GrapeVertexPartition[VD : ClassTag](val pid : Int,
                                          val vm : GraphXVertexMap[Long,Long],
                                          val vertexData: VertexDataStore[VD],
                                          val client : VineyardClient,
                                          val routingTable: RoutingTable,
                                          var bitSet: BitSet = null) extends Logging {
  val startLid = 0
  val endLid = vm.innerVertexSize()
  def partVnum : Long = endLid - startLid
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
      var lid = 0
      val limit: VertexId = vm.innerVertexSize()
      override def hasNext: Boolean = {
        lid < limit
      }

      override def next(): (VertexId, VD) = {
        val res = (vm.innerVertexLid2Oid(lid), getData(lid))
        lid += 1;
        res
      }
    }
  }
  def generateVertexDataMessage : Iterator[(PartitionID, VertexDataMessage[VD])] = {
    val res = new ArrayBuffer[(PartitionID,VertexDataMessage[VD])]()
    val curFid = vm.fid()
    val idParser = new IdParser(vm.fnum())
    for (i <- 0 until(routingTable.numPartitions)){
      val lids = routingTable.get(i)
      if (lids != null){
        val gids = new Array[Long](lids.length)
        val newData = new Array[VD](lids.length)
        var j = 0
        while (j < lids.length){
          gids(j) = idParser.generateGlobalId(curFid, lids(j))
          require(lids(j) < endLid)
          newData(j) = getData(lids(j))
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
    while (vertexDataMessage.hasNext){
      val (dstPid, msg) = vertexDataMessage.next()
      require(dstPid == pid)
      val outerGids = msg.gids
      val outerDatas = msg.newData
      val outerLids = new Array[Long](outerGids.length)
      val idParser = new IdParser(vm.fnum())
      var i = 0
      val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
      while (i < outerGids.length){
        require(vm.outerVertexGid2Vertex(outerGids(i), vertex))
        outerLids(i) = vertex.GetValue()
//        log.info(s"Partition ${pid} received outer vdata updating info ${outerLids(i)}, ${outerDatas(i)}")
        vertexData.setData(outerLids(i), outerDatas(i))
        i += 1
      }
    }
    this
  }

  def map[VD2: ClassTag](f: (VertexId, VD) => VD2): GrapeVertexPartition[VD2] = {
    // Construct a view of the map transformation
    val newValues = PrimitiveArray.create(GrapeUtils.getRuntimeClass[VD2], vm.getVertexSize.toInt).asInstanceOf[PrimitiveArray[VD2]]
    var i = bitSet.nextSetBit(0)
    while (i >= 0) {
      newValues.set(i, f(vm.getId(i), getData(i)))
      i = bitSet.nextSetBit(i + 1)
    }
    this.withNewValues(new InHeapVertexDataStore[VD2](newValues, client))
  }

  def filter(pred: (VertexId, VD) => Boolean): GrapeVertexPartition[VD] = {
    //    // Allocate the array to store the results into
    val newMask = new BitSet(partVnum.toInt)

    // Iterate over the active bits in the old mask and evaluate the predicate
    var curLid = bitSet.nextSetBit(startLid)
    while (curLid >= 0 && curLid < endLid) {
      if (pred(vm.getId(curLid), getData(curLid))){
        log.info(s"vertex lid ${curLid}(oid ${vm.getId(curLid)} matches pred")
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
    val newValues = PrimitiveArray.create(GrapeUtils.getRuntimeClass[VD2], partVnum.toInt).asInstanceOf[PrimitiveArray[VD2]]

    iter.foreach { product =>
      val oid = product._1
      val vdata = product._2
      require(vm.getVertex(oid,vertex))
      val lid = vertex.GetValue().toInt
      if (lid >= 0) {
        if (newMask.get(lid)) {
          newValues.set(lid,reduceFunc(newValues.get(lid), vdata))
        } else { // otherwise just store the new value
          newMask.set(lid)
          newValues.set(lid, vdata)
        }
      }
    }
    this.withNewValues(new InHeapVertexDataStore[VD2](newValues,client)).withMask(newMask)
  }

  /** Hides the VertexId's that are the same between `this` and `other`. */
  def minus(other: GrapeVertexPartition[VD]): GrapeVertexPartition[VD] = {
    if (this.vm != other.vm) {
      logWarning("Minus operations on two VertexPartitions with different indexes is slow.")
      minus(createUsingIndex(other.iterator))
    } else {
      this.withMask(this.bitSet.andNot(other.bitSet))
    }
  }

  def diff(other: GrapeVertexPartition[VD]): GrapeVertexPartition[VD] = {
    if (this.vm != this.vm) {
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
    if (this.vm != other.vm){
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {
      val time0 = System.nanoTime()
      val newValues = PrimitiveArray.create(GrapeUtils.getRuntimeClass[VD3], partVnum.toInt).asInstanceOf[PrimitiveArray[VD3]]
      var i = this.bitSet.nextSetBit(0)
      while (i >= 0 && i < partVnum) {
        val otherV: Option[VD2] = if (other.bitSet.get(i)) Some(other.getData(i)) else None
        newValues.set(i, f(this.vm.getId(i), this.getData(i), otherV))
        i = this.bitSet.nextSetBit(i + 1)
      }
      val time1 = System.nanoTime()
      log.info(s"Left join cost ${(time1 - time0) / 1000000} ms")
      this.withNewValues(new InHeapVertexDataStore[VD3](newValues,client))
    }
  }

  /**
   * Similar effect as aggregateUsingIndex((a, b) => a)
   */
  def createUsingIndex[VD2: ClassTag](iter: Iterator[Product2[VertexId, VD2]])
  : GrapeVertexPartition[VD2] = {
    val newMask = new BitSet(partVnum.toInt)
    val newValues = PrimitiveArray.create(GrapeUtils.getRuntimeClass[VD2], partVnum.toInt).asInstanceOf[PrimitiveArray[VD2]]
    iter.foreach { pair =>
//      val pos = self.index.getPos(pair._1)
      val vertexFound = vm.getVertex(pair._1,vertex)
      if (vertexFound){
        val lid = vertex.GetValue().toInt
        newMask.set(lid)
        newValues.set(lid, pair._2)
      }
    }
    this.withNewValues(new InHeapVertexDataStore[VD2](newValues,client)).withMask(newMask)
  }

  /** Inner join another VertexPartition. */
  def innerJoin[U: ClassTag, VD2: ClassTag]
  (other: GrapeVertexPartition[U])
  (f: (VertexId, VD, U) => VD2): GrapeVertexPartition[VD2] = {
    if (this.vm != other.vm){
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      innerJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newMask = this.bitSet & other.bitSet
      val newValues = PrimitiveArray.create(GrapeUtils.getRuntimeClass[VD2], partVnum.toInt).asInstanceOf[PrimitiveArray[VD2]]
      var i = newMask.nextSetBit(startLid)
      while (i >= 0) {
        newValues.set(i, f(this.vm.getId(i), this.getData(i), other.getData(i)))
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewValues(new InHeapVertexDataStore[VD2](newValues,client)).withMask(newMask)
    }
  }

  def withNewValues[VD2 : ClassTag](vds: VertexDataStore[VD2]) : GrapeVertexPartition[VD2] = {
    new GrapeVertexPartition[VD2](pid, vm, vds,client, routingTable, bitSet)
  }

  def withMask(newMask: BitSet): GrapeVertexPartition[VD] ={
    new GrapeVertexPartition[VD](pid, vm, vertexData, client,routingTable, newMask)
  }

  override def toString: String = "GrapeVertexPartition{" + "pid=" + pid + ",startLid=" + startLid + ", endLid=" + endLid + '}'
}

class GrapeVertexPartitionBuilder[VD: ClassTag] extends Logging{
  private val vertexDataBuilder : VertexDataBuilder[Long,VD] = ScalaFFIFactory.newVertexDataBuilder[VD]()

  def init(fragVnums : Long, value : VD): Unit ={
    vertexDataBuilder.init(fragVnums,value)
    log.info(s"Init vertex data with ${fragVnums} ${value}")
  }

  def build(pid : Int, client : VineyardClient, vertexMap: GraphXVertexMap[Long,Long], routingTable: RoutingTable) : GrapeVertexPartition[VD] = {
    val vertexData = vertexDataBuilder.seal(client).get()
    log.info(s"Partition ${pid} built vertex data ${vertexData}")
    require(vertexMap.getVertexSize == vertexData.verticesNum(), s"csr inner vertex should equal to vmap ${vertexMap.innerVertexSize()}, ${vertexData.verticesNum()}")
    //copy to heap
    val newArray = PrimitiveArray.create(GrapeUtils.getRuntimeClass[VD], vertexData.verticesNum().toInt).asInstanceOf[PrimitiveArray[VD]]
    var i = 0
    val limit = vertexData.verticesNum()
    while (i < limit){
      newArray.set(i, vertexData.getData(i))
      i += 1
    }
    val newVertexData = new InHeapVertexDataStore[VD](newArray,client)
    new GrapeVertexPartition[VD](pid, vertexMap,newVertexData, client, routingTable)
  }
}
