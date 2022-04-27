package org.apache.spark.graphx

import com.alibaba.graphscope.graph.{GraphXVertexIdManager, VertexDataManager}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GrapeVertexPartition[VD : ClassTag] (pid: Int, numPartitions: Int,
                            idManager: GraphXVertexIdManager, vertexDataManager: VertexDataManager[VD]) extends Logging {
  val totalVnum: Long = idManager.innerVerticesNum
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

  override def toString: String = "JavaVertexPartition{" + "idManager=" + idManager + ", vertexDataManager=" + vertexDataManager + ", startLid=" + startLid + ", endLid=" + endLid + ", pid=" + pid + '}'

}
