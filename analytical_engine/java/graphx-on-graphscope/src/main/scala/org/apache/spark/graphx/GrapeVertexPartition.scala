package org.apache.spark.graphx

import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.traits.{GraphXVertexIdManager, VertexDataManager}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

class GrapeVertexPartition[VD : ClassTag] (pid: Int, numPartitions: Int,
                                           val idManager: GraphXVertexIdManager,
                                           val vertexDataManager: VertexDataManager[VD],
                                           var mask : BitSet = null) extends Logging {

  val totalVnum: Long = idManager.getInnerVerticesNum
  val chunkSize: Long = (totalVnum + (numPartitions - 1)) / numPartitions
  val startLid: VertexId = Math.min(chunkSize * pid, totalVnum)
  val endLid: VertexId = Math.min(startLid + chunkSize, totalVnum)
  val partitionVnum: VertexId = endLid - startLid
  if (mask == null){
    mask = new BitSet(totalVnum.toInt)
    mask.setUntil(endLid.toInt)
  }
  log.info(s"Creating GrapeVertexPartition ${this} active vertices: ${mask.cardinality()}")

  def iterator : Iterator[(VertexId,VD)] = {
    new Iterator[(VertexId,VD)]{
      private var curLid = startLid
      override def hasNext: Boolean = {
        curLid = mask.nextSetBit(curLid.toInt)
        curLid < endLid && curLid >= 0 && curLid >= startLid
      }

      override def next(): (VertexId, VD) = {
        val res = (idManager.lid2Oid(curLid), vertexDataManager.getVertexData(curLid))
        curLid += 1
        res
      }
    }
  }

  def map[VD2: ClassTag](f: (VertexId, VD) => VD2): GrapeVertexPartition[VD2] = {
    // Construct a view of the map transformation
//    val newValues = new Array[Object](totalVnum.toInt)
    val newValues = new Array[VD2](totalVnum.toInt)
    var i = startLid.toInt
    while (i < totalVnum) {
      newValues(i) = f(idManager.lid2Oid(i), vertexDataManager.getVertexData(i))
      i += 1
    }
    this.withNewValues(newValues)
  }

  def filter(pred: (VertexId, VD) => Boolean): GrapeVertexPartition[VD] = {
    // Allocate the array to store the results into
    val newMask = new BitSet(totalVnum.toInt)
    // Iterate over the active bits in the old mask and evaluate the predicate
    var i = mask.nextSetBit(startLid.toInt)
    while (i >= 0 && i < endLid) {
      if (pred(idManager.lid2Oid(i), vertexDataManager.getVertexData(i))){
        log.info(s"vertex lid ${i} ${idManager.lid2Oid(i)} matches")
        newMask.set(i)
      }
      i = mask.nextSetBit(i + 1)
    }
    this.withMask(newMask)
  }

  def aggregateUsingIndex[VD2: ClassTag](
                                          iter: Iterator[Product2[VertexId, VD2]],
                                          reduceFunc: (VD2, VD2) => VD2): GrapeVertexPartition[VD2] = {
    val newMask = new BitSet(totalVnum.toInt)
    val newValues = new Array[VD2](totalVnum.toInt)
    iter.foreach { product =>
      val vid = product._1
      val vdata = product._2
      val lid = idManager.oid2Lid(vid).toInt
//      val pos = self.index.getPos(vid)
      if (lid >= 0) {
        if (newMask.get(lid)) {
          newValues(lid) = reduceFunc(newValues(lid), vdata)
        } else { // otherwise just store the new value
          newMask.set(lid)
          newValues(lid) = vdata
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
      var i = newMask.nextSetBit(startLid.toInt)
      while (i >= 0 && i < endLid) {
        if (this.vertexDataManager.getVertexData(i) == other.vertexDataManager.getVertexData(i)) {
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewVertexData(other.vertexDataManager).withMask(newMask)
    }
  }

  def leftJoin[VD2: ClassTag, VD3: ClassTag]
  (other: GrapeVertexPartition[VD2])
  (f: (VertexId, VD, Option[VD2]) => VD3): GrapeVertexPartition[VD3] = {
    if (this.idManager != other.idManager) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newValues = new Array[VD3](totalVnum.toInt)
      require(this.startLid == other.startLid, "start lid should match")
      require(this.endLid == other.endLid, "end lid should match")
      var i = this.mask.nextSetBit(startLid.toInt)
      while (i >= 0 && i < endLid) {
        val otherV: Option[VD2] = if (other.mask.get(i)) Some(other.vertexDataManager.getVertexData(i)) else None
        newValues(i) = f(this.idManager.lid2Oid(i), this.vertexDataManager.getVertexData(i), otherV)
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
    val newMask = new BitSet(totalVnum.toInt)
    val newValues = new Array[VD2](totalVnum.toInt)
    iter.foreach { pair =>
//      val pos = self.index.getPos(pair._1)
      val lid = idManager.oid2Lid(pair._1).toInt
      if (lid >= 0) {
        newMask.set(lid)
        newValues(lid) = pair._2
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
      val newValues = new Array[VD2](totalVnum.toInt)
      var i = newMask.nextSetBit(startLid.toInt)
      while (i >= 0) {
        newValues(i) = f(this.idManager.lid2Oid(i), this.vertexDataManager.getVertexData(i), other.vertexDataManager.getVertexData(i))
        i = newMask.nextSetBit(i + 1)
      }
      this.withNewValues(newValues).withMask(newMask)
    }
  }

  def withNewValues[VD2 : ClassTag](vds: Array[VD2]) : GrapeVertexPartition[VD2] = {
    new GrapeVertexPartition[VD2](pid, numPartitions, idManager, vertexDataManager.withNewVertexData[VD2](vds), mask)
  }

  def withNewVertexData[VD2 : ClassTag](newVertexDataManager: VertexDataManager[VD2]) : GrapeVertexPartition[VD2] = {
    new GrapeVertexPartition[VD2](pid, numPartitions, idManager, newVertexDataManager, mask)
  }

  def withMask(newMask: BitSet): GrapeVertexPartition[VD] ={
    new GrapeVertexPartition(pid, numPartitions, idManager, vertexDataManager, newMask)
  }

  override def toString: String = "JavaVertexPartition{" + "idManager=" + idManager + ", vertexDataManager=" + vertexDataManager + ", startLid=" + startLid + ", endLid=" + endLid + ", pid=" + pid + '}'

}
