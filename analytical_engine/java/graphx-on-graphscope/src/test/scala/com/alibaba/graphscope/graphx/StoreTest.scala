package com.alibaba.graphscope.graphx

import com.alibaba.graphscope.graphx.store.{AbstractDataStore, InHeapVertexDataStore}
import com.alibaba.graphscope.graphx.utils.SerializationUtils
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

//class MyRunnable(val tid : Int,val tnum : Int, val store : InHeapVertexDataStore[Long], val resArray : Array[AbstractDataStore[Int]]) extends Runnable{
//  override def run(): Unit = {
//    val len = store.length
//    val chunk = len / tnum
//    val begin = chunk * tid
//    val newStore = store.getOrCreate[Int](tid).getOrCreate[Int](tid).getOrCreate[Int](tid)
//    val end = Math.min(begin + chunk, len)
//    for (i <- begin until end){
//      newStore.setData(i, tid)
//    }
//    resArray(tid) = newStore
//  }
//}
@RunWith(classOf[JUnitRunner])
class StoreTest extends FunSuite {
//  test("test serialization"){
//    val store = new InHeapVertexDataStore[Long](1,30,3)
//    val resArray = new Array[AbstractDataStore[Int]](3)
//    val threads = new Array[Thread](3)
//    for (i <- 0 until 3){
//      val thread = new Thread(new MyRunnable(i,3,store,resArray))
//      thread.start()
//      threads(i) = thread
//    }
//    for (i <- 0 until 3){
//      threads(i).join();
//    }
//    val ind = 0 until 30
//    for (i <- 0 until 3){
//      System.out.println(resArray(i))
//    }
//    val res = ind.map(i => resArray(0).getData(i))
//    System.out.println(res.mkString(","))
//  }
}
