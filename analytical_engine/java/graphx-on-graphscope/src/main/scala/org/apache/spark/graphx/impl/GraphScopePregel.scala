package org.apache.spark.graphx.impl

import com.alibaba.graphscope.graphx.SerializationUtils
import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.graphx.utils.ExecutorUtils
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}
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
  val msgClass: Class[MSG] = classTag[MSG].runtimeClass.asInstanceOf[java.lang.Class[MSG]]
  val vdClass: Class[VD] = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
  val edClass: Class[ED] = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]

  def run(): Graph[VD,ED] = {
    if (!graph.isInstanceOf[GrapeGraphImpl[VD,ED]]) {
      log.error("Only support grape graph")
      return graph
    }
    //0. write back vertex.
    val grapeGraph = graph.asInstanceOf[GrapeGraphImpl[VD,ED]]
    //1. serialization
    log.info("[Driver:] start serialization functions.")
    SerializationUtils.write(vprog, VPROG_SERIALIZATION_PATH)
    SerializationUtils.write(sendMsg, SEND_MSG_SERIALIZATION_PATH)
    SerializationUtils.write(mergeMsg, MERGE_MSG_SERIALIZATION_PATH)

    //launch mpi processes. and run.
    val t0 = System.nanoTime()

    /** Generate a json string contains necessary info to reconstruct a graphx graph, can be like
     * workerName:*/
    val vmIds = grapeGraph.generateGlobalVMIds()
    val csrIds = grapeGraph.generateCSRIds()
    val vdataIds = grapeGraph.generateVdataIds()
    log.info(s"[GraphScopePregel]: collect distinct ids vm: ${vmIds}, csr: ${csrIds}, vdata: ${vdataIds}")

    MPIUtils.launchGraphX[MSG,VD,ED](vmIds,csrIds,vdataIds,
      msgClass, vdClass, edClass,
      VPROG_SERIALIZATION_PATH, SEND_MSG_SERIALIZATION_PATH, MERGE_MSG_SERIALIZATION_PATH,
      initialMsg,maxIteration)
    //FIXME: reconstruct the graph from result ids.

    val t1 = System.nanoTime()
    log.info(s"[GraphScopePregel: ] running MPI process cost :${(t1 - t0) / 1000000} ms")
    graph
  }
}
