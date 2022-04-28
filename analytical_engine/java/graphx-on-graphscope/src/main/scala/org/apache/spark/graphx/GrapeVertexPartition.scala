package org.apache.spark.graphx

import org.apache.spark.graphx.traits.{VertexDataManager, GraphXVertexIdManager}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GrapeVertexPartition[VD : ClassTag] (pid: Int, numPartitions: Int,
                                           idManager: GraphXVertexIdManager, vertexDataManager: VertexDataManager[VD]) extends Logging {
  val totalVnum: Long = idManager.getInnerVerticesNum
  val chunkSize: Long = (totalVnum + (numPartitions - 1)) / numPartitions
  val startLid = Math.min(chunkSize * pid, totalVnum)
  val endLid = Math.min(startLid + chunkSize, totalVnum)
  log.info("Creating GrapeVertexPartition {}", this)

  def iterator : Iterator[(VertexId,VD)] = {
    new Iterator[(VertexId,VD)]{
      private var curLid = startLid
      override def hasNext: Boolean = {
        curLid < endLid
      }

      override def next(): (VertexId, VD) = {
        val res = (idManager.lid2Oid(curLid).asInstanceOf[VertexId], vertexDataManager.getVertexData(curLid))
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
      newValues(i) =  f(idManager.lid2Oid(i), vertexDataManager.getVertexData(i))
      i += 1
    }
    this.withNewValues(newValues)
  }

  def withNewValues[VD2 : ClassTag](vds: Array[VD2]) : GrapeVertexPartition[VD2] = {
    new GrapeVertexPartition[VD2](pid, numPartitions, idManager, vertexDataManager.withNewVertexData[VD2](vds))
  }

  override def toString: String = "JavaVertexPartition{" + "idManager=" + idManager + ", vertexDataManager=" + vertexDataManager + ", startLid=" + startLid + ", endLid=" + endLid + ", pid=" + pid + '}'

}
