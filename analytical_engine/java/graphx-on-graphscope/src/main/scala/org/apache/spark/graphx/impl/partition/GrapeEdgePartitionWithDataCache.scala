package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.graphx.{GraphXCSR, GraphXVertexMap, VineyardClient}
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag

/** Note that we convert the ed type of csr, but we should not invoke any use of csr.getEdata. */
class GrapeEdgePartitionWithDataCache[VD: ClassTag, ED: ClassTag]
(pid : Int, csr : GraphXCSR[Long,ED], vm : GraphXVertexMap[Long,Long], client : VineyardClient,
            edgeReversed : Boolean, activeSet : BitSet = null, val edataArrayCache : PrimitiveArray[ED])
  extends GrapeEdgePartition[VD,ED](pid, csr, vm, client, edgeReversed, activeSet){
  require(edataArrayCache.size() == csr.getEdataArray.getLength)

  /**
   * override to provide in-java store.
   * @param eid edge id, not offset.
   *  @return
   */
  override def getEdata(eid : Long) : ED = {
    edataArrayCache.get(eid)
  }

  override def reverse: GrapeEdgePartition[VD, ED] = {
    new GrapeEdgePartitionWithDataCache[VD,ED](pid, csr, vm, client,!edgeReversed, activeSet,edataArrayCache)
  }

  override def toString: String = "GrapeEdgePartitionWithDataCache(pid=" + pid +
    ", start lid" + startLid + ", end lid " + endLid + ",csr: " + csr + ", vm" + vm.toString +
    ",out edges num" + partOutEdgeNum + ", in edges num" + partInEdgeNum +")"
}
