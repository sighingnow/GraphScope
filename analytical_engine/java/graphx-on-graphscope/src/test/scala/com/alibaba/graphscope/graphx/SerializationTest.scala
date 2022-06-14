package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.graphx.SerializationUtils.logger
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import java.io.{File, FileInputStream, IOException, ObjectInputStream, ObjectStreamClass}

@RunWith(classOf[JUnitRunner])
class SerializationTest extends FunSuite{
  test("test serialization"){
    val vprog : (Long,Long,Long) => Long = {
      (a,b,c) => a
    }
    SerializationUtils.write("/tmp/vprog-tmp",vprog)
    println("success in serialization")
  }

  test("serialization & deserialization"){
    val value = 1
    val vprog : (Long,Long,Long) => Long = {
      (a,b,c) => a + value
    }
    val filePath = "/tmp/vprog-tmp"
    SerializationUtils.write(filePath,vprog)

    val func = SerializationUtils.read(getClass.getClassLoader,"/tmp/vprog-tmp").asInstanceOf[Array[Object]]
    require(func.length == 1)
    val funcCasted = func(0).asInstanceOf[(Long,Long,Long)=>Long]
  }
}
