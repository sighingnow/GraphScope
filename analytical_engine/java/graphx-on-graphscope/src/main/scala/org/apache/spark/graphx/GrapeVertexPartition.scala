package org.apache.spark.graphx

import org.apache.spark.graphx.traits.{GraphXVertexIdManager, VertexDataManager}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

class GrapeVertexPartition[VD : ClassTag] (pid: Int, numPartitions: Int,
                                           idManager: GraphXVertexIdManager, vertexDataManager: VertexDataManager[VD],
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
        curLid < endLid && curLid > 0 && curLid >= startLid
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

  def withNewValues[VD2 : ClassTag](vds: Array[VD2]) : GrapeVertexPartition[VD2] = {
    new GrapeVertexPartition[VD2](pid, numPartitions, idManager, vertexDataManager.withNewVertexData[VD2](vds), mask)
  }

  def withMask(newMask: BitSet): GrapeVertexPartition[VD] ={
    new GrapeVertexPartition(pid, numPartitions, idManager, vertexDataManager, newMask)
  }

  override def toString: String = "JavaVertexPartition{" + "idManager=" + idManager + ", vertexDataManager=" + vertexDataManager + ", startLid=" + startLid + ", endLid=" + endLid + ", pid=" + pid + '}'

}
