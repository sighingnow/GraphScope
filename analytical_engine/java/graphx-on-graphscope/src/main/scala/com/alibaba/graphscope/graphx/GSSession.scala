package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.graphx.GSSession.{GS_PYTHON_DIR, RES_PATTERN, SAFE_WORD}
import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.GraphXFragmentStructure
import com.alibaba.graphscope.utils.PythonInterpreter
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.grape.GrapeGraphImpl
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.reflect.ClassTag

/** A wrapper for GraphScope python session. */
class GSSession(sc : SparkContext) extends Logging{
  val graph2GraphName : mutable.HashMap[GrapeGraphImpl[_,_], String] = new mutable.HashMap[GrapeGraphImpl[_,_], String]()
  val pythonInterpreter = new PythonInterpreter
  pythonInterpreter.init()
  log.info("Successfully init GraphScope session")
  //cd to graphscope dir
  pythonInterpreter.runCommand("import os\nos.chdir(\"" + GS_PYTHON_DIR +"\")")


  /**
   *
   * @param cmd python command in string
   * @param resultVariable the result variable from which we will extract host_ids_str and frag_name
   * @return
   */
  def loadGraph[VD: ClassTag, ED : ClassTag](cmd : String, resultVariable : String) : GrapeGraphImpl[VD,ED] = {
    //this command should re
    pythonInterpreter.runCommand(cmd)
//    val lastCommand = s"res_str:${resultVariable}.template_str + \";\" + ${resultVariable}.host_ids_str"
    val lastCommand = "\"res_str:\"+" + resultVariable + ".template_str + \";\"+" + resultVariable + ".host_ids_str"
    pythonInterpreter.runCommand(lastCommand)
    var rawRes = pythonInterpreter.getMatched(RES_PATTERN)
    if (rawRes.startsWith("\'") || rawRes.endsWith("\"")){
      rawRes = rawRes.substring(1)
    }
    if (rawRes.endsWith("\'") || rawRes.endsWith("\"")){
      rawRes = rawRes.substring(0, rawRes.length - 1)
    }
    val resStr = rawRes.substring(rawRes.indexOf(RES_PATTERN) + RES_PATTERN.length + 1)
    val splited = resStr.split(";")
    require(splited.length == 2, "resutl str can't be splited into two parts")
    val fragName = splited(0)
    val hostIds = splited(1)
    //as this graph can later be used to run in graphscope session, we need to keep the matching between
    //java object and vineyard objectId,
    val res = GraphScopeHelper.loadFragmentAsGraph[VD,ED](sc, hostIds, fragName)
    graph2GraphName(res) = resultVariable
    res
  }

  /**
   * Run on GAE with returned graph
   */
  def runOnGAE[VD: ClassTag,ED : ClassTag](cmd : String, Graph : GrapeGraphImpl[VD,ED], resultVar : String) : GrapeGraphImpl[VD,ED] = {
    null
  }

  def runOnGAE[VD: ClassTag,ED : ClassTag](cmd : String, graph : Graph[VD,ED]) : Unit = {
    graph match {
      case grapeGraph :  GrapeGraphImpl[VD,ED] => runOnGAEImpl(cmd, grapeGraph)
      case graphXGraph : GraphImpl[VD,ED] => runOnGAEImpl(cmd, graphXGraph)
    }
  }

  /** Run on GAE without returning graph */
  private def runOnGAEImpl[VD: ClassTag, ED: ClassTag](cmd : String, graph : GrapeGraphImpl[VD,ED]) : Unit = {
    if (graph2GraphName.contains(graph)){
      val graphVariable = graph2GraphName(graph)
      log.info(s"try to run ${graphVariable} on GAE")
      //here we suppose there is only one {}.
      if (cmd.indexOf("{}") == cmd.lastIndexOf("{}")){
        log.info("only one {} in string, proceed...")
        val tmpCmd = cmd.replace("{}", graphVariable)
        log.info(s"After replacement, running cmd ${tmpCmd}")
        val resCmd = tmpCmd + "\n" + "print(" + SAFE_WORD + ")\n"
        pythonInterpreter.runCommand(resCmd)
        log.info("waiting for safe word...")
        pythonInterpreter.getMatched(SAFE_WORD)
        log.info("Found safe word in output stream.")
      }
      else {
        log.error("Found multiple {} in string, not expected...")
      }
    }
    else {
      log.info(s"Graph ${graph} is not a original graph in this session, trying to dump to vineyard")
      /**
       * For grapeGraphImpl which reaches here, there are three possible scenarios
       * - Graph is original loaded via graphScope, and ported to spark, wrapped as RDD. Some
       *   operators have been applied to it, the graph structure not changed, but data changed.
       *   In this case the underlying fragment is projected fragment.
       * - Graph is original loaded in spark, and converted to grapeGraph. In this case, the underlying
       * fragment is graphx fragment.
       *  */
      if (graph.backend.equals(GraphXFragmentStructure)){
        //Launch mpi process and construct graphxFragment,seal to vineyard.
        log.info("Launching mpi processes to construct graphxFragment")
      }
      else {
        log.info("Got ArrowProjectedFragment backend graph, need to create a new projected fragment")
      }
    }
  }

  /**
   * User better should
   */
  private def runOnGAEImpl[VD: ClassTag, ED: ClassTag](cmd : String, graph : GraphImpl[VD,ED]) : Unit = {
    val grapeGraph = GraphScopeHelper.graph2Fragment(graph)
    runOnGAEImpl(cmd, grapeGraph)
  }

  def close() : Unit = {
    pythonInterpreter.close()
    log.info("GS session closed")
  }
}

object GSSession{
  val GS_PYTHON_DIR = "/home/graphscope/gs/python";
  val RES_PATTERN = "res_str";
  //A safe word which we append to the execution of python code, its appearance in
  // output stream, indicating command has been successfully executoed.
  val SAFE_WORD = "Spark-GraphScope-OK"
}
