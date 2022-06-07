package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.utils.PythonInterpreter
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

/** A wrapper for GraphScope python session. */
class GSSession(sc : SparkContext) extends Logging{
  val pythonInterpreter = new PythonInterpreter
  pythonInterpreter.init()
  log.info("Successfully init GraphScope session")


  def close() : Unit = {
    pythonInterpreter.close()
    log.info("GS session closed")
  }
}
