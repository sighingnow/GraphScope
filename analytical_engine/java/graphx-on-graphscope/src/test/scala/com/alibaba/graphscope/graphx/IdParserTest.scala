package com.alibaba.graphscope.graphx

import org.apache.spark.graphx.impl.partition.IdParser
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IdParserTest extends FunSuite {
  test("test1"){
    val idParser = new IdParser(4)
    val gid1 = idParser.generateGlobalId(0, 1)
    val gid2 = idParser.generateGlobalId(3, 101)
    val gid3 = idParser.generateGlobalId(2, 10000001)
    println(s"${gid1}, ${gid2}, ${gid3}")
    assert(idParser.getFragId(gid1) == 0)
    assert(idParser.getFragId(gid2) == 3)
    assert(idParser.getFragId(gid3) == 2)
    assert(idParser.getLocalId(gid1) == 1)
    assert(idParser.getLocalId(gid2) == 101)
    assert(idParser.getLocalId(gid3) == 10000001)
  }
  test("test2"){
    val idParser = new IdParser(1)
    val gid1 = idParser.generateGlobalId(0, 1)
    val gid2 = idParser.generateGlobalId(0, 2)
    assert(idParser.getFragId(gid1) == 0)
    assert(idParser.getFragId(gid2) == 0)
    assert(idParser.getLocalId(gid1) == 1)
    assert(idParser.getLocalId(gid2) == 2)
  }

}
