package com.alibaba.graphscope.graphx

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

@RunWith(classOf[JUnitRunner])
class CaseClassTest extends FunSuite{
  def write[A](obj: A): Array[Byte] = {
    val bo = new ByteArrayOutputStream()
    new ObjectOutputStream(bo).writeObject(obj)
    bo.toByteArray
  }

  def read(bytes:Array[Byte]): Any = {
    new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject()
  }

  test("test1"){
    println(read(write( {a:Int => a+1} )).asInstanceOf[ Function[Int,Int] ](5)) // == 6
  }

}
