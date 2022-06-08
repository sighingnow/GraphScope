package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.graphx.GSSession.{GS_PYTHON_DIR, RES_PATTERN}
import com.alibaba.graphscope.utils.PythonInterpreter
import org.apache.spark.SparkContext
import org.apache.spark.graphx.impl.GrapeGraphImpl
import org.apache.spark.graphx.rdd.GraphScopeRDD
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

/** A wrapper for GraphScope python session. */
class GSSession(sc : SparkContext) extends Logging{
  val pythonInterpreter = new PythonInterpreter
  pythonInterpreter.init()
  log.info("Successfully init GraphScope session")
  //cd to graphscope dir
  pythonInterpreter.runCommand("import os\nos.chdir(\"" + GS_PYTHON_DIR +"\")")


  def run[VD: ClassTag, ED : ClassTag](cmd : String) : GrapeGraphImpl[VD,ED] = {
    //this command should re
    pythonInterpreter.runCommand(cmd)
    val resStr = pythonInterpreter.getMatched(RES_PATTERN)
    val splited = resStr.split(";")
    require(splited.length == 2, "resutl str can't be splited into two parts")
    val fragName = splited(0)
    val hostIds = splited(1)
    GraphScopeRDD.loadFragmentAsGraph(sc, hostIds,fragName)
  }

  def close() : Unit = {
    pythonInterpreter.close()
    log.info("GS session closed")
  }
}

object GSSession{
  val GS_PYTHON_DIR = "/home/graphscope/gs/python";
  val RES_PATTERN = "res_str";
}
