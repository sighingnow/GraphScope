package org.apache.spark.graphx.impl.graph

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import org.apache.spark.graphx.GraphXConf
import org.apache.spark.graphx.traits.GraphXVertexIdManager
import org.apache.spark.internal.Logging


class GraphXVertexIdManagerImpl[VD,ED](val conf: GraphXConf[VD,ED], fragment: IFragment[Long,Long,_,_]) extends GraphXVertexIdManager with Logging{

  val innerVerticesNum: Long = fragment.getInnerVerticesNum
  val fragVerticesNum: Long = fragment.getVerticesNum
  val lid2Oids = new Array[Long](fragVerticesNum.toInt)
  private val oid2Lids: Long2LongOpenHashMap = new Long2LongOpenHashMap()
  val vertex: Vertex[Long] = FFITypeFactoryhelper.newVertexLong.asInstanceOf[Vertex[Long]]
  var lid = 0
  while (lid < fragVerticesNum) {
    vertex.SetValue(lid)
    val oid: Long = fragment.getId(vertex)
    lid2Oids(lid) = oid
    oid2Lids.put(oid, lid)
    lid += 1
  }
  log.info(s"Finish VertexId manager construction: fragId ${fragment.fid()}, vertices ${fragment.getVerticesNum}")


  override def lid2Oid(lid: Long): Long = {
    lid2Oids(lid.toInt)
  }

  override def oid2Lid(oid: Long): Long = {
    oid2Lids.getOrDefault(oid, -(1L))
  }

  override def getInnerVerticesNum: Long = {
    innerVerticesNum
  }

  override def getVerticesNum: Long = {
    fragVerticesNum
  }
}
