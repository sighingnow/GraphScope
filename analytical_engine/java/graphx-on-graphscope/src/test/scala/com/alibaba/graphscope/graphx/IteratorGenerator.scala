package com.alibaba.graphscope.graphx

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.reflect.ClassTag

@RunWith(classOf[JUnitRunner])
class IteratorGenerator extends FunSuite{
  def generateIterator(lid : Long): Iterator[(Long,Long)] = {
    Iterator((lid, lid))
  }

  test("test1"){
    val t1 = System.nanoTime()
    var i = 0L;
    var res = 0L;
    while (i < 100000000) {
      val iterator = generateIterator(i)
      while (iterator.hasNext){
        val tuple = iterator.next()
        res += tuple._1 + tuple._2
      }
      i += 1;
    }
    val t2 = System.nanoTime()
    println("test1 iterate over iterator : "+ (t2 - t1) / 1000000 + "ms")
  }
  test("test2"){
    val t1 = System.nanoTime()
    var i = 0L;
    var res = 0L;
    var iter = Iterator.empty
    while (i < 100000000) {
      val iterator = generateIterator(i)
      iterator.foreach( tuple => res += tuple._1 + tuple._2)
      i += 1;
    }
    val t2 = System.nanoTime()
    println("test2 iterate over iterator : "+ (t2 - t1) / 1000000 + "ms")
  }
  test("test2"){
    val t1 = System.nanoTime()
    var i = 0L;
    var res = 0L;
    var iter = Iterator.empty
    var arr = Array.empty[(Long,Long)]
    while (i < 100000000) {
      val iterator = generateIterator(i)
//      val arr = iterator.toArray
      arr.
      iterator.foreach( tuple => res += tuple._1 + tuple._2)
      i += 1;
    }
    val t2 = System.nanoTime()
    println("test2 iterate over iterator : "+ (t2 - t1) / 1000000 + "ms")
  }
}
