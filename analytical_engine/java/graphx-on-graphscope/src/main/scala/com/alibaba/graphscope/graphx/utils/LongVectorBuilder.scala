package com.alibaba.graphscope.graphx.utils

import com.alibaba.graphscope.graphx.utils.LongVectorBuilder.initSize
import com.alibaba.graphscope.stdcxx.StdVector

class LongVectorBuilder() {
  val data : StdVector[Long] = ScalaFFIFactory.newLongVector
  data.resize(initSize)
  var curSize = 0

  def add(value : Long) : Unit = {
    check
    data.set(curSize, value)
    curSize += 1
  }

  def finish() : StdVector[Long] = {
    data.resize(curSize)
    data
  }

  def check : Unit = {
    if (curSize >= data.size()){
      val oldSize = data.size()
      data.resize(oldSize * 2)
    }
  }
}
object LongVectorBuilder{
  val initSize = 64
}
