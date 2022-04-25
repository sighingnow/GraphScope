package org.apache.spark.graphx.impl

import com.alibaba.graphscope.graphx.SerializationUtils
import com.alibaba.graphscope.utils.MPIUtils
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
  val VDATA_MAPPED_PATH = "/tmp/graphx-vdata"
  val msgClass = classTag[MSG].runtimeClass.asInstanceOf[java.lang.Class[MSG]]

  def run(): Unit = {
    if (!graph.isInstanceOf[GrapeGraphImpl[VD,ED]]) {
      log.error("Only support grape graph")
      return
    }
    val grapeGraph = graph.asInstanceOf[GrapeGraphImpl[VD, ED]]
    //Persist and distribute functions.
    log.info("[Driver:] start serialization functions.")
    SerializationUtils.write(vprog, VPROG_SERIALIZATION_PATH)
    SerializationUtils.write(sendMsg, SEND_MSG_SERIALIZATION_PATH)
    SerializationUtils.write(mergeMsg, MERGE_MSG_SERIALIZATION_PATH)
    //Distribute them to all workers.
//    MPIUtils.distribute(VPROG_SERIALIZATION_PATH, SEND_MSG_SERIALIZATION_PATH, MERGE_MSG_SERIALIZATION_PATH)
//    log.info("[Driver:] Finish distribution")

    //launch mpi processes. and run.
    val t0 = System.nanoTime()
    MPIUtils.launchGraphX[MSG](grapeGraph.fragIds, initialMsg, msgClass, maxIteration, VPROG_SERIALIZATION_PATH,
      SEND_MSG_SERIALIZATION_PATH, MERGE_MSG_SERIALIZATION_PATH, VDATA_MAPPED_PATH, grapeGraph.numVertices * 16L)
    val t1 = System.nanoTime();
    log.info(s"[Driver:] Running graphx pie cost: ${(t1 - t0) / 1000000} ms")
    //update the result to graph for a new graph.
  }
}
