package org.apache.spark.graphx.test

import org.apache.spark.graphx.utils.GrapeEdgePartitionRegistry
import org.apache.spark.internal.Logging

class Main extends Logging{
  def main(array: Array[String]): Unit = {
    val registry = GrapeEdgePartitionRegistry.getOrCreate[Int,Int]
    registry.createArrayBuilder(0)
    registry.createArrayBuilder(1)
    log.info("finish create builders")

    val edges = Array((1L,1L,1),(1L,2L,3),(2L,3L,4),(3L,4L,5))

    val (srcBuilder,dstBuilder,edataBuilder) = registry.getBuilders()
    srcBuilder.reserve(edges.size)
    dstBuilder.reserve(edges.size)
    edataBuilder.reserve(edges.size)
    val partIter = edges.iterator
    while (partIter.hasNext){
      val edge = partIter.next()
      srcBuilder.unsafeAppend(edge._1)
      dstBuilder.unsafeAppend(edge._2)
      edataBuilder.unsafeAppend(edge._3)
    }
    log.info("finish builders")

    registry.constructEdgePartition(0)
    registry.constructEdgePartition(1)

    val p1 = registry.getEdgePartitionWrapper(0)
    val p2 = registry.getEdgePartitionWrapper(1)
    log.info(s"wrapper 1 : ${p1}, wrapper 2 ${p2}")
    val oids = p1.oids
    for (i <- 0 until( oids.getLength.toInt)){
      log.info(s"oid ${i} is ${oids.get(i)}")
    }
  }

}
