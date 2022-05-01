package org.apache.spark.graphx.impl

import com.alibaba.graphscope.graphx.SerializationUtils
import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, FragmentRegistry, Graph, VertexId}
import org.apache.spark.internal.Logging

import scala.reflect.{ClassTag, classTag}

class GraphScopePregel[VD: ClassTag, ED: ClassTag, MSG: ClassTag]
(graph: Graph[VD, ED], initialMsg: MSG, maxIteration: Int, activeDirection: EdgeDirection, vprog: (VertexId, VD, MSG) => VD,
 sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, MSG)],
 mergeMsg: (MSG, MSG) => MSG) extends Logging {
  val VPROG_SERIALIZATION_PATH = "/tmp/graphx-vprog"
  val SEND_MSG_SERIALIZATION_PATH = "/tmp/graphx-sendMsg"
  val MERGE_MSG_SERIALIZATION_PATH = "/tmp/graphx-mergeMsg"
  val VDATA_MAPPED_PATH = "graphx-vdata"
  val msgClass = classTag[MSG].runtimeClass.asInstanceOf[java.lang.Class[MSG]]
  val vdClass = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
  val edClass = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]

  def run(): Graph[VD,ED] = {
    if (!graph.isInstanceOf[GrapeGraphImpl[VD,ED]]) {
      log.error("Only support grape graph")
      return graph
    }
    //0. write back vertex.
    val grapeGraph = graph.asInstanceOf[GrapeGraphImpl[VD,ED]]
      val vdataMappedSize = grapeGraph.numVertices * 16L + 8L
    grapeGraph.vertices.writeBackVertexData(VDATA_MAPPED_PATH, vdataMappedSize)
    log.info(s"[Driver:] Finish write back vdata mapped size: ${vdataMappedSize} to ${VDATA_MAPPED_PATH}")
    //1. serialization
    log.info("[Driver:] start serialization functions.")
    SerializationUtils.write(vprog, VPROG_SERIALIZATION_PATH)
    SerializationUtils.write(sendMsg, SEND_MSG_SERIALIZATION_PATH)
    SerializationUtils.write(mergeMsg, MERGE_MSG_SERIALIZATION_PATH)

//launch mpi processes. and run.
    val t0 = System.nanoTime()

    MPIUtils.launchGraphX[MSG,VD,ED](FragmentRegistry.getFragIds, initialMsg, msgClass, vdClass, edClass, maxIteration, VPROG_SERIALIZATION_PATH,
      SEND_MSG_SERIALIZATION_PATH, MERGE_MSG_SERIALIZATION_PATH, VDATA_MAPPED_PATH, vdataMappedSize)
    val t1 = System.nanoTime()
    log.info(s"[Driver:] Running graphx pie cost: ${(t1 - t0) / 1000000} ms")
    graph
//    //update the result to graph for a new graph.
//
//    log.info(s"[Driver:] Writing back vertex data")
//    val resVertices = grapeGraph.vertices.copyAndUpdateVertexData(VDATA_MAPPED_PATH, vdata_mapped_size).cache()
//    GrapeGraphImpl.fromExistingRDDs(resVertices, grapeGraph.edges, grapeGraph.fragIds).cache()
  }
}
