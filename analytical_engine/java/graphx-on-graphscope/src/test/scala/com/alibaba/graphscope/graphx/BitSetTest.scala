package com.alibaba.graphscope.graphx

import org.apache.spark.util.collection.BitSet
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BitSetTest extends FunSuite{
  test("test1"){
    val bitSet = new BitSet(8546613)
    bitSet.setUntil(8546612)
    var ind = 0;
    val time0 = System.nanoTime()
    while (ind > 0){
      ind = bitSet.nextSetBit(ind + 1)
    }
    val time1 = System.nanoTime()
    System.out.println(s"Bitset iter cost ${(time1 - time0) / 1000000} ms")
  }
}
