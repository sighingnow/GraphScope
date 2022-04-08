package com.alibaba.graphscope.graphx

import org.apache.spark.graphx.{EdgeTriplet, VertexId}

class FunctionWrapper[VD,ED, A](vprog : (VertexId, VD, A) => VD, sendMsg : EdgeTriplet[VD, ED] => Iterator[(VertexId, A)]) extends Serializable {
  def closureFunction[E,T1,T2,T3,T4](enclosed: E)(gen: E => ((T1,T2,T3) => T4)) = gen(enclosed)
  val partialVprog = closureFunction(()){
    enclosed => (vid : VertexId, vd : VD, a :A) => vprog(vid, vd, a)
  }
}
