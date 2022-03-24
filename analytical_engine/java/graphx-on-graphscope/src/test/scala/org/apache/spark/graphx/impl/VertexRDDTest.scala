package org.apache.spark.graphx.impl

import com.alibaba.graphscope.graph.IdManager
import org.apache.spark.SparkContext
import org.scalatest._

class VertexRDDTest extends FunSuite {
  val sc : SparkContext = new SparkContext()
  val oids  = Array(1, 2, 3)
  val vdata = Array(2, 4, 6)
  val idManager : IdManager = new IdManagerImpl()
  val vertices = new VertexRDDImpl[Int](sc, oids.length, idManager, vdata)
  test("test size"){
    assert(vertices.count() == 3)
  }

  test("test map"){
    // test map
    val newVertices = vertices.mapValues( vdata => vdata.toDouble)
    assert(!vertices.equals(newVertices))
    assert(newVertices.count() == 3)
  }

//  test("test take"){
//    assert(vertices.take(1)(0)._1 == 1)
//  }

}
