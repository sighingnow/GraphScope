package org.apache.spark.graphx.test

import org.apache.spark.graphx.impl.partition.{EdgeShuffle, EdgeShuffleReceived}
import org.apache.spark.graphx.utils.{ExecutorUtils, GrapeEdgePartitionRegistry}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

object EdgeMain extends Logging{
  def main(array: Array[String]): Unit = {
    ExecutorUtils.registerPartition(0)
    val registry = GrapeEdgePartitionRegistry.getOrCreate[Int,Int]
    val oids = new OpenHashSet[Int]
    oids.add(1)
    oids.add(3)
    oids.add(5)
    val srcs = Array(1L, 1L, 3L)
    val dsts = Array(3L, 5L, 5L)
    val attrs = Array(1, 2 ,3)

    val edgeShuffle = new EdgeShuffle[Int](0, 0, oids.getBitSet, srcs, dsts, attrs)
    val edgeShuffleReceived = new EdgeShuffleReceived[Int](1, 0)
    edgeShuffleReceived.set(0, edgeShuffle)
    registry.addEdgesToBuilder(0, edgeShuffleReceived)
    registry.build(0)
  }

}
