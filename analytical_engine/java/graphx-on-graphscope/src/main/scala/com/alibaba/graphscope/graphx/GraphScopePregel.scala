package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.graphx.utils.SerializationUtils
import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}
import org.apache.spark.internal.Logging

import scala.reflect.{ClassTag, classTag}

class GraphScopePregel[VD: ClassTag, ED: ClassTag, MSG: ClassTag]
(sc: SparkContext, graph: Graph[VD, ED], initialMsg: MSG, maxIteration: Int, activeDirection: EdgeDirection, vprog: (VertexId, VD, MSG) => VD,
 sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, MSG)],
 mergeMsg: (MSG, MSG) => MSG) extends Logging {
  val SERIAL_PATH = "/tmp/graphx-meta"
  val msgClass: Class[MSG] = classTag[MSG].runtimeClass.asInstanceOf[java.lang.Class[MSG]]
  val vdClass: Class[VD] = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
  val edClass: Class[ED] = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]

  def run(): Graph[VD,ED] = {
    //Can accept both grapeGraph or GraphXGraph
    val grapeGraph = GraphScopeHelper.graph2Fragment(graph)
    //0. write back vertex.
    //1. serialization
    log.info("[Driver:] start serialization functions.")
    SerializationUtils.write(SERIAL_PATH, vdClass, edClass, msgClass, vprog, sendMsg, mergeMsg, initialMsg, sc.appName,activeDirection)

    //launch mpi processes. and run.
    val t0 = System.nanoTime()

    //FIXME: Support running projected fragment
    /** Generate a json string contains necessary info to reconstruct a graphx graph, can be like
     * workerName:*/
    val fragIds = grapeGraph.fragmentIds.collect()
    log.info(s"[GraphScopePregel]: Collected frag ids ${fragIds.mkString(",")}")

    //running pregel will not change vertex data type.
    MPIUtils.launchGraphX[MSG,VD,ED](fragIds,vdClass,edClass,msgClass, SERIAL_PATH,maxIteration)
    //usually we need to construct graph vertices attributes from vineyard array.

    val t1 = System.nanoTime()
    log.info(s"[GraphScopePregel: ] running MPI process cost :${(t1 - t0) / 1000000} ms")
    //FIXME: return the updated frag.
    grapeGraph
  }
}
