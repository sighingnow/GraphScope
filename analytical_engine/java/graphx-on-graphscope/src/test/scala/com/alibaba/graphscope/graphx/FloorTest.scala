package com.alibaba.graphscope.graphx

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import java.util

@RunWith(classOf[JUnitRunner])
class FloorTest extends FunSuite{
  test("test1"){
    val map = new util.TreeMap[Long,Int]()
    val arrays = Array(Array(0,1),Array(2,3,4),Array(5))
    var tmp = 0;
    for (ind <- arrays.indices){
      map.put(tmp, ind)
      tmp += arrays(ind).length
    }

    assert(map.floorEntry(0L).getValue.equals(0))
    assert(map.floorEntry(1L).getValue.equals(0))
    assert(map.floorEntry(2L).getValue.equals(1))
    assert(map.floorEntry(4L).getValue.equals(1))
    assert(map.floorEntry(5L).getValue.equals(2))
  }
}
