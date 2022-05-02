package org.apache.spark.graphx

import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.traits.{GraphXVertexIdManager, VertexDataManager}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

class GrapeVertexPartition[VD : ClassTag] (pid: Int, numPartitions: Int,
                                           val idManager: GraphXVertexIdManager,
                                           val values : Array[VD],
                                           val startLid : Long,
                                           val endLid : Long,
                                           var mask : BitSet = null) extends Logging {
  val totalVnum: Long = idManager.getInnerVerticesNum
//  val chunkSize: Long = (totalVnum + (numPartitions - 1)) / numPartitions
//  val startLid: VertexId = Math.min(chunkSize * pid, totalVnum)
//  val endLid: VertexId = Math.min(startLid + chunkSize, totalVnum)
  val partitionVnum: VertexId = endLid - startLid

  if (mask == null){
    mask = new BitSet(partitionVnum.toInt)
    mask.setUntil(partitionVnum.toInt)
  }

  log.info(s"Creating GrapeVertexPartition ${this} active vertices: ${mask.cardinality()}, startLid ${startLid}, endLid ${endLid}")

//  def initValues(vertexDataManager: VertexDataManager[VD], len : Int) : Unit = {
//    val values = new Array[VD](len)
//    var i = 0
//    while (i < partitionVnum){
//      values(i) = vertexDataManager.getVertexData(i + startLid)
//      i += 1
//    }
//  }
  def iterator : Iterator[(VertexId,VD)] = {
    new Iterator[(VertexId,VD)]{
      private var offset = 0
      override def hasNext: Boolean = {
        offset = mask.nextSetBit(offset)
        offset < partitionVnum && offset >= 0
      }

      override def next(): (VertexId, VD) = {
        val res = (idManager.lid2Oid(startLid + offset), values(offset))
        offset += 1
        res
      }
    }
  }

  def map[VD2: ClassTag](f: (VertexId, VD) => VD2): GrapeVertexPartition[VD2] = {
    // Construct a view of the map transformation
//    val newValues = new Array[Object](totalVnum.toInt)
    val newValues = new Array[VD2](partitionVnum.toInt)
    var i = mask.nextSetBit(0)
    while (i >= 0) {
      newValues(i) = f(idManager.lid2Oid(i + startLid.toInt), values(i))
      i = mask.nextSetBit(i + 1)
    }
    this.withNewValues(newValues)
  }

  def filter(pred: (VertexId, VD) => Boolean): GrapeVertexPartition[VD] = {
    // Allocate the array to store the results into
    val newMask = new BitSet(partitionVnum.toInt)
    // Iterate over the active bits in the old mask and evaluate the predicate
    var offset = mask.nextSetBit(0)
    while (offset >= 0 && offset < partitionVnum) {
      if (pred(idManager.lid2Oid(offset + startLid), values(offset))){
        log.info(s"vertex lid ${offset + startLid} ${idManager.lid2Oid(offset + startLid)} matches")
        newMask.set(offset)
      }
      offset = mask.nextSetBit(offset + 1)
    }
    this.withMask(newMask)
  }

  def aggregateUsingIndex[VD2: ClassTag](
                                          iter: Iterator[Product2[VertexId, VD2]],
                                          reduceFunc: (VD2, VD2) => VD2): GrapeVertexPartition[VD2] = {
    val newMask = new BitSet(partitionVnum.toInt)
    val newValues = new Array[VD2](partitionVnum.toInt)
    iter.foreach { product =>
      val vid = product._1
      val vdata = product._2
      val lid = idManager.oid2Lid(vid).toInt
//      val pos = self.index.getPos(vid)
      if (lid >= 0) {
        val lidOffset = lid - startLid.toInt
        if (newMask.get(lidOffset)) {
          newValues(lidOffset) = reduceFunc(newValues(lidOffset), vdata)
        } else { // otherwise just store the new value
          newMask.set(lidOffset)
          newValues(lidOffset) = vdata
        }
      }
    }
    this.withNewValues(newValues).withMask(newMask)
  }

  /** Hides the VertexId's that are the same between `this` and `other`. */
  def minus(other: GrapeVertexPartition[VD]): GrapeVertexPartition[VD] = {
    if (this.idManager != other.idManager) {
      logWarning("Minus operations on two VertexPartitions with different indexes is slow.")
      minus(createUsingIndex(other.iterator))
    } else {
      this.withMask(this.mask.andNot(other.mask))
    }
  }

  def diff(other: GrapeVertexPartition[VD]): GrapeVertexPartition[VD] = {
    if (this.idManager != this.idManager) {
      logWarning("Diffing two VertexPartitions with different indexes is slow.")
      diff(createUsingIndex(other.iterator))
    } else {
      require(this.startLid == other.startLid, "start lid should match")
      require(this.endLid == other.endLid, "end lid should match")
      val newMask = this.mask & other.mask
      var i = newMask.nextSetBit(0)
      while (i >= 0 && i < partitionVnum) {
        if (values(i) == other.values(i)) {
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewValues(other.values).withMask(newMask)
    }
  }

  def leftJoin[VD2: ClassTag, VD3: ClassTag]
  (other: GrapeVertexPartition[VD2])
  (f: (VertexId, VD, Option[VD2]) => VD3): GrapeVertexPartition[VD3] = {
    if (this.idManager != other.idManager) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newValues = new Array[VD3](partitionVnum.toInt)
      require(this.startLid == other.startLid, "start lid should match")
      require(this.endLid == other.endLid, "end lid should match")
      var i = this.mask.nextSetBit(0)
      while (i >= 0 && i < partitionVnum) {
        val otherV: Option[VD2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(this.idManager.lid2Oid(i + startLid), this.values(i), otherV)
        i = this.mask.nextSetBit(i + 1)
      }
      this.withNewValues(newValues)
    }
  }

  /**
   * Similar effect as aggregateUsingIndex((a, b) => a)
   */
  def createUsingIndex[VD2: ClassTag](iter: Iterator[Product2[VertexId, VD2]])
  : GrapeVertexPartition[VD2] = {
    val newMask = new BitSet(partitionVnum.toInt)
    val newValues = new Array[VD2](partitionVnum.toInt)
    iter.foreach { pair =>
//      val pos = self.index.getPos(pair._1)
      val lid = idManager.oid2Lid(pair._1).toInt
      if (lid >= 0) {
        newMask.set(lid - startLid.toInt)
        newValues(lid - startLid.toInt) = pair._2
      }
    }
    this.withNewValues(newValues).withMask(newMask)
  }

  /** Inner join another VertexPartition. */
  def innerJoin[U: ClassTag, VD2: ClassTag]
  (other: GrapeVertexPartition[U])
  (f: (VertexId, VD, U) => VD2): GrapeVertexPartition[VD2] = {
    if (this.idManager != other.idManager) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      innerJoin(createUsingIndex(other.iterator))(f)
    } else {
      require(this.startLid == other.startLid, "start lid should match")
      require(this.endLid == other.endLid, "end lid should match")
      val newMask = this.mask & other.mask
      val newValues = new Array[VD2](partitionVnum.toInt)
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        newValues(i) = f(this.idManager.lid2Oid(i + startLid.toInt), this.values(i), other.values(i))
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewValues(newValues).withMask(newMask)
    }
  }

  def withNewValues[VD2 : ClassTag](vds: Array[VD2]) : GrapeVertexPartition[VD2] = {
    new GrapeVertexPartition[VD2](pid, numPartitions, idManager, vds, startLid, endLid, mask)
  }

//  def withNewVertexData[VD2 : ClassTag](newVertexDataManager: VertexDataManager[VD2]) : GrapeVertexPartition[VD2] = {
//    new GrapeVertexPartition[VD2](pid, numPartitions, idManager, newVertexDataManager, mask)
//  }

  def withMask(newMask: BitSet): GrapeVertexPartition[VD] ={
    new GrapeVertexPartition(pid, numPartitions, idManager, values, startLid, endLid, newMask)
  }

  override def toString: String = "JavaVertexPartition{" + "idManager=" + idManager + ", startLid=" + startLid + ", endLid=" + endLid + ", pid=" + pid + '}'

}
