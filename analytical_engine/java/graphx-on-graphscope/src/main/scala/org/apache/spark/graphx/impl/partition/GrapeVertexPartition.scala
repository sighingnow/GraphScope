package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.graphx.{GraphXVertexMap, VertexData, VertexDataBuilder}
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.utils.{ExecutorUtils, GrapeVertexPartitionRegistry, ScalaFFIFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

class GrapeVertexPartition[VD : ClassTag](val pid : Int, val startLid : Long, val endLid : Long,
                                          val vm : GraphXVertexMap[Long,Long],
                                          val vertexData: VertexData[Long,VD]){
  def partVnum : Long = endLid - startLid
  def iterator : Iterator[(VertexId,VD)] = {
    null
  }

  def filter(pred: (VertexId, VD) => Boolean): GrapeVertexPartition[VD] = {
    //    // Allocate the array to store the results into
    //    val newMask = new BitSet(partitionVnum.toInt)
    //    // Iterate over the active bits in the old mask and evaluate the predicate
    //    var offset = mask.nextSetBit(0)
    //    while (offset >= 0 && offset < partitionVnum) {
    //      if (pred(idManager.lid2Oid(offset + startLid), values(offset))){
    //        log.info(s"vertex lid ${offset + startLid} ${idManager.lid2Oid(offset + startLid)} matches")
    //        newMask.set(offset)
    //      }
    //      offset = mask.nextSetBit(offset + 1)
    //    }
    //    this.withMask(newMask)
    null
  }

  def aggregateUsingIndex[VD2: ClassTag](
                                          iter: Iterator[Product2[VertexId, VD2]],
                                          reduceFunc: (VD2, VD2) => VD2): GrapeVertexPartition[VD2] = {
    //    val newMask = new BitSet(partitionVnum.toInt)
    //    val newValues = new Array[VD2](partitionVnum.toInt)
    //    iter.foreach { product =>
    //      val vid = product._1
    //      val vdata = product._2
    //      val lid = idManager.oid2Lid(vid).toInt
    ////      val pos = self.index.getPos(vid)
    //      if (lid >= 0) {
    //        val lidOffset = lid - startLid.toInt
    //        if (newMask.get(lidOffset)) {
    //          newValues(lidOffset) = reduceFunc(newValues(lidOffset), vdata)
    //        } else { // otherwise just store the new value
    //          newMask.set(lidOffset)
    //          newValues(lidOffset) = vdata
    //        }
    //      }
    //    }
    //    this.withNewValues(newValues).withMask(newMask)
    null
  }

  /** Hides the VertexId's that are the same between `this` and `other`. */
  def minus(other: GrapeVertexPartition[VD]): GrapeVertexPartition[VD] = {
    //    if (this.idManager != other.idManager) {
    //      logWarning("Minus operations on two VertexPartitions with different indexes is slow.")
    //      minus(createUsingIndex(other.iterator))
    //    } else {
    //      this.withMask(this.mask.andNot(other.mask))
    //    }
    null
  }

  def diff(other: GrapeVertexPartition[VD]): GrapeVertexPartition[VD] = {
    //    if (this.idManager != this.idManager) {
    //      logWarning("Diffing two VertexPartitions with different indexes is slow.")
    //      diff(createUsingIndex(other.iterator))
    //    } else {
    //      require(this.startLid == other.startLid, "start lid should match")
    //      require(this.endLid == other.endLid, "end lid should match")
    //      val newMask = this.mask & other.mask
    //      var i = newMask.nextSetBit(0)
    //      while (i >= 0 && i < partitionVnum) {
    //        if (values(i) == other.values(i)) {
    //          newMask.unset(i)
    //        }
    //        i = newMask.nextSetBit(i + 1)
    //      }
    //      this.withNewValues(other.values).withMask(newMask)
    //    }
    null
  }

  def leftJoin[VD2: ClassTag, VD3: ClassTag]
  (other: GrapeVertexPartition[VD2])
  (f: (VertexId, VD, Option[VD2]) => VD3): GrapeVertexPartition[VD3] = {
    //    if (this.idManager != other.idManager) {
    //      logWarning("Joining two VertexPartitions with different indexes is slow.")
    //      leftJoin(createUsingIndex(other.iterator))(f)
    //    } else {
    //      val newValues = new Array[VD3](partitionVnum.toInt)
    //      require(this.startLid == other.startLid, "start lid should match")
    //      require(this.endLid == other.endLid, "end lid should match")
    //      var i = this.mask.nextSetBit(0)
    //      while (i >= 0 && i < partitionVnum) {
    //        val otherV: Option[VD2] = if (other.mask.get(i)) Some(other.values(i)) else None
    //        newValues(i) = f(this.idManager.lid2Oid(i + startLid), this.values(i), otherV)
    //        i = this.mask.nextSetBit(i + 1)
    //      }
    //      this.withNewValues(newValues)
    //    }
    null
  }

  /**
   * Similar effect as aggregateUsingIndex((a, b) => a)
   */
  def createUsingIndex[VD2: ClassTag](iter: Iterator[Product2[VertexId, VD2]])
  : GrapeVertexPartition[VD2] = {
    //    val newMask = new BitSet(partitionVnum.toInt)
    //    val newValues = new Array[VD2](partitionVnum.toInt)
    //    iter.foreach { pair =>
    ////      val pos = self.index.getPos(pair._1)
    //      val lid = idManager.oid2Lid(pair._1).toInt
    //      if (lid >= 0) {
    //        newMask.set(lid - startLid.toInt)
    //        newValues(lid - startLid.toInt) = pair._2
    //      }
    //    }
    //    this.withNewValues(newValues).withMask(newMask)
    null
  }

  /** Inner join another VertexPartition. */
  def innerJoin[U: ClassTag, VD2: ClassTag]
  (other: GrapeVertexPartition[U])
  (f: (VertexId, VD, U) => VD2): GrapeVertexPartition[VD2] = {
    //    if (this.idManager != other.idManager) {
    //      logWarning("Joining two VertexPartitions with different indexes is slow.")
    //      innerJoin(createUsingIndex(other.iterator))(f)
    //    } else {
    //      require(this.startLid == other.startLid, "start lid should match")
    //      require(this.endLid == other.endLid, "end lid should match")
    //      val newMask = this.mask & other.mask
    //      val newValues = new Array[VD2](partitionVnum.toInt)
    //      var i = newMask.nextSetBit(0)
    //      while (i >= 0) {
    //        newValues(i) = f(this.idManager.lid2Oid(i + startLid.toInt), this.values(i), other.values(i))
    //        i = newMask.nextSetBit(i + 1)
    //      }
    //      this.withNewValues(newValues).withMask(newMask)
    //    }
    null
  }

  def withNewValues[VD2 : ClassTag](vds: Array[VD2]) : GrapeVertexPartition[VD2] = {
    //    new GrapeVertexPartition[VD2](pid, numPartitions, idManager, vds, startLid, endLid, mask)
    null
  }

  def withNewValues[VD2 : ClassTag](vdataMappedPath : String, size : Long) : GrapeVertexPartition[VD2] = {
    //    val newArray = new Array[VD2](partitionVnum.toInt)
    //    val registry = SharedMemoryRegistry.getOrCreate()
    //    val buffer = registry.tryMapFor(vdataMappedPath, size)
    //    val totalLength = buffer.readLong(0)
    //    log.info(s"grape vertex partition ${pid} mapped buffer ${buffer} for ${vdataMappedPath} of size ${size}, length ${totalLength}")
    //
    //    require(totalLength >= endLid, s"total length should be greater than lid ${totalLength} vs ${endLid}")
    //    val dstVdClass = GrapeUtils.getRuntimeClass[VD2]
    //    val chunksize = GrapeUtils.bytesForType(dstVdClass)
    //    var curAddr = 8L + chunksize * startLid
    //    var ind = 0
    //    val endAddr = 8L + chunksize * endLid
    //    if (dstVdClass.equals(classOf[Long]) || dstVdClass.eq(classOf[java.lang.Long])){
    //      while (curAddr < endAddr){
    //        newArray(ind) = buffer.readLong(curAddr).asInstanceOf[VD2]
    //        curAddr += chunksize
    //        ind += 1
    //      }
    //    }
    //    else if (dstVdClass.equals(classOf[Double]) || dstVdClass.eq(classOf[java.lang.Double])){
    //      while (curAddr < endAddr){
    //        newArray(ind) = buffer.readDouble(curAddr).asInstanceOf[VD2]
    //        curAddr += chunksize
    //        ind += 1
    //      }
    //    }
    //    else if (dstVdClass.equals(classOf[Int]) || dstVdClass.eq(classOf[java.lang.Integer])){
    //      while (curAddr < endAddr){
    //        newArray(ind) = buffer.readInt(curAddr).asInstanceOf[VD2]
    //        curAddr += chunksize
    //        ind += 1
    //      }
    //    }
    //    else {
    //      throw new IllegalStateException(s"Unrecognized clz ${dstVdClass.getName}, byte per ele ${chunksize}")
    //    }
    //    log.info(s"updated new array for ${startLid} to ${endLid} ${newArray.mkString("Array(", ", ", ")")}")
    //    new GrapeVertexPartition[VD2](pid, numPartitions, idManager, newArray, startLid, endLid, mask)
    null
  }

  //  def withNewVertexData[VD2 : ClassTag](newVertexDataManager: VertexDataManager[VD2]) : GrapeVertexPartition[VD2] = {
  //    new GrapeVertexPartition[VD2](pid, numPartitions, idManager, newVertexDataManager, mask)
  //  }

  def withMask(newMask: BitSet): GrapeVertexPartition[VD] ={
    //    new GrapeVertexPartition(pid, numPartitions, idManager, values, startLid, endLid, newMask)
    null
  }

  override def toString: String = "GrapeVertexPartition{" + "pid=" + pid + ",startLid=" + startLid + ", endLid=" + endLid + '}'

}

class GrapeVertexPartitionBuilder[VD: ClassTag] extends Logging{
  private var vertexDataBuilder : VertexDataBuilder[Long,VD] = ScalaFFIFactory.newVertexDataBuilder[VD]()
  private var vertexData : VertexData[Long,VD] = null.asInstanceOf[VertexData[Long,VD]]
//  private var initialized = false;
  private var built = false
//  private var cleared = false

  /** this func should be called when we need to reset this vertex builder, and a build a new vertex
   * partition. */
//  def clear(pid : Int) : Unit = {
//    if (cleared){
//      log.info(s"Partition ${pid} want to clear, but already cleared");
//      return ;
//    }
//    cleared = true
//    built = false
//    initialized = false
//    vertexData = null
//    vertexDataBuilder = ScalaFFIFactory.newVertexDataBuilder[VD]()
//    log.info(s"Partition ${pid} create new vd builder ${vertexDataBuilder}")
//  }

  /** Concurrency constrol should be done in registry level */
  def init(fragVnums : Long, value : VD): Unit ={
    vertexDataBuilder.init(fragVnums,value)
    log.info(s"Init vertex data with ${fragVnums} ${value}")
  }

  def init[VD_OLD: ClassTag](vertexPartition : GrapeVertexPartition[VD_OLD], map : (VertexId, VD_OLD) => VD) : Unit = {
    val vm = vertexPartition.vm
    val oldVdArray = vertexPartition.vertexData.getVdataArray
    require(vm.getVertexSize == oldVdArray.getLength, s"vm frag vnum not equal to vd array len ${vm.getVertexSize} vs ${oldVdArray.getLength}")
    val newVdArrayBuilder = ScalaFFIFactory.newArrowArrayBuilder(GrapeUtils.getRuntimeClass[VD]).asInstanceOf[ArrowArrayBuilder[VD]]
    newVdArrayBuilder.reserve(vm.getVertexSize)
    var i = 0L;
    val ivnum = vm.innerVertexSize()
    while (i < ivnum){
      val oid = vm.innerVertexLid2Oid(i)
      val oldVD = oldVdArray.get(i)
      newVdArrayBuilder.unsafeAppend(map(oid,oldVD))
      i += 1
    }
    val fragVnum = vm.getVertexSize
    while (i < fragVnum){
      val oid = vm.outerVertexLid2Oid(i)
      val oldVD = oldVdArray.get(i)
      newVdArrayBuilder.unsafeAppend(map(oid,oldVD))
      i += 1
    }
    log.info("Finish building new vd array")
    vertexDataBuilder.init(newVdArrayBuilder)
  }

  def build(pid : Int) : Unit = {
    vertexData = vertexDataBuilder.seal(ExecutorUtils.getVineyarClient).get()
    log.info(s"Partition ${pid} built vertex data ${vertexData}")
    ExecutorUtils.setVertexData(vertexData)
    built = true
  }

  def getResult : VertexData[Long,VD] = {
    require(vertexData != null)
    vertexData
  }

  def getVertexPartition(pid : Int) : GrapeVertexPartition[VD] = {
    val localPartNum = ExecutorUtils.getPartitionNum
    val grapePartId = ExecutorUtils.graphXPid2GrapePid(pid)
    log.info(s"got partition ${pid}'s corresponding grape partition ${grapePartId}")
    val vertexMap = ExecutorUtils.getGlobalVM
    require(vertexMap.getVertexSize == vertexData.verticesNum(), s"csr inner vertex should equal to vmap ${vertexMap.innerVertexSize()}, ${vertexData.verticesNum()}")
    val ivnum = vertexMap.innerVertexSize()
    val chunkSize = (ivnum + localPartNum - 1) / localPartNum
    val begin = chunkSize * grapePartId
    val end = Math.min(begin + chunkSize, ivnum)
    log.info(s"Part ${pid}, grape Pid ${grapePartId} got range (${begin}, ${end})")
    new GrapeVertexPartition[VD](pid, begin, end, vertexMap,vertexData)
  }

//  def isInitialized :Boolean = initialized

  def isBuilt : Boolean = built

//  def isCleared : Boolean = cleared
}
