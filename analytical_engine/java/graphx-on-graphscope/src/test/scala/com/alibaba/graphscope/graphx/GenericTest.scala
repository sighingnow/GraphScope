package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.utils.VertexImpl
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GenericTest extends FunSuite{
  test("test1"){
    val vertex = new VertexImpl[Long](1)
    assert(vertex.GetValue().equals(1L), "vertex value should be 1")
    val longVertex = vertex.asInstanceOf[VertexImpl[java.lang.Long]]
    assert(longVertex.GetValue().equals(1L.asInstanceOf[java.lang.Long]))
  }

}
