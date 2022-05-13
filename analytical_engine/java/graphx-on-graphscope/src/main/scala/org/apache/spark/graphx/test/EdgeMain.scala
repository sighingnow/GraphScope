package org.apache.spark.graphx.test

import com.alibaba.graphscope.utils.MPIUtils
import org.apache.spark.graphx.impl.partition.{EdgeShuffle, EdgeShuffleReceived}
import org.apache.spark.graphx.utils.{ExecutorUtils, GrapeEdgePartitionRegistry, GrapeVertexPartitionRegistry}
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

object EdgeMain extends Logging{
  def main(array: Array[String]): Unit = {
    ExecutorUtils.registerPartition(0)
    val registry = GrapeEdgePartitionRegistry.getOrCreate[Int,Int]
    val oids = new OpenHashSet[Long]
    oids.add(1)
    oids.add(3)
    oids.add(5)
    log.info(s"oids size: ${oids.size}")
    val srcs = Array(1L, 1L, 3L)
    val dsts = Array(3L, 5L, 5L)
    val attrs = Array(1, 2 ,3)

    val edgeShuffle = new EdgeShuffle[Int](0, 0, oids, srcs, dsts, attrs)
    val edgeShuffleReceived = new EdgeShuffleReceived[Int](1, 0)
    edgeShuffleReceived.set(0, edgeShuffle)
    registry.addEdgesToBuilder(0, edgeShuffleReceived)
    registry.buildLocalVertexMap(0)
    log.info(s"local vm: ${ExecutorUtils.getHost2LocalVMID()}")
    val res = MPIUtils.constructGlobalVM(ExecutorUtils.getHost2LocalVMID(),ExecutorUtils.endPoint, "int64_t", "uint64_t")
    ExecutorUtils.setGlobalVMIDs(res)
    log.info(s"global vm: ${ExecutorUtils.getGlobalVMID}")
    registry.buildCSR(0)
    log.info(s"csr id ${ExecutorUtils.getGraphXCSR.id()}")
    log.info(s"graphx vm id ${ExecutorUtils.getGlobalVM.id()}");

    val vertexRegistry = GrapeVertexPartitionRegistry.getOrCreate[Int]
    vertexRegistry.checkPrerequisite(0)
    vertexRegistry.init(0, 1)
    vertexRegistry.build(0)
    val vertexPartition = vertexRegistry.getVertexPartition(0)
    log.info(s"${vertexPartition}")
  }
}
