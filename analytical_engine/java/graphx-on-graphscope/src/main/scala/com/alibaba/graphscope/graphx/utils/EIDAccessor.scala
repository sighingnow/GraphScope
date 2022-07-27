package com.alibaba.graphscope.graphx.utils

import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime

class EIDAccessor(var address : Long) {
  @inline
  def getEid(offset : Int) : Long = {
    JavaRuntime.getLong(address + (offset << 4) + 8)
  }
}
