package com.alibaba.graphscope.app

import org.apache.spark.graphx.{EdgeTriplet, VertexId}

/**
 *  * Scala trait acts like java interface. We define three key abstract method, user need to override
 * them to provide core computation logic.
 *
 * @tparam VD vertex data
 * @tparam A message
 */
trait GraphXAppBase[VD,ED,A] {
  def vprog() : (VertexId, VD, A) => VD

  def sendMsg() : EdgeTriplet[VD, ED] => Iterator[(VertexId, A)]

  def mergeMsg() : (A,A) => A
}
