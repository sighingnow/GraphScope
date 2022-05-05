package com.alibaba.graphscope.graphx

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Serialization extends FunSuite{
  test("test serialization"){
    val vprog : (Long,Long,Long) => Long = {
      (a,b,c) => a
    }
    SerializationUtils.write(vprog, "/tmp/vprog-tmp")
    println("success in serialization")
  }

  test("serialization & deserialization"){
    val value = 1
    val vprog : (Long,Long,Long) => Long = {
      (a,b,c) => a + value
    }
    SerializationUtils.write(vprog, "/tmp/vprog-tmp")
    val func = SerializationUtils.read("/tmp-vprog-tmp")
    val funcCasted = func.asInstanceOf[(Long,Long,Long)=>Long]
    assert(funcCasted(1,1,1).equals(2), s"not equal ${funcCasted(1,1,1)}")
  }
}
