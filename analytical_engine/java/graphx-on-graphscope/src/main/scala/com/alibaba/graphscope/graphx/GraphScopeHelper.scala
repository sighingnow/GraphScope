package com.alibaba.graphscope.graphx

import org.apache.spark.SparkContext

object GraphScopeHelper {
  /**
   * Creating GSSession, one spark context can have many graphscope session.
   */
  def createSession(sc : SparkContext) : GSSession = {
    new GSSession(sc)
  }
}
