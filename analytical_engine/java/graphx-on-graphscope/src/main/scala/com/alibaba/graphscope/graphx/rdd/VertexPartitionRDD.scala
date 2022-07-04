package com.alibaba.graphscope.graphx.rdd

import com.alibaba.graphscope.graphx.rdd.impl.{GrapeEdgePartition, GrapeVertexPartition}
import org.apache.spark.{Dependency, OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class VertexPartitionRDD[VD : ClassTag](sc : SparkContext, val grapePartitions : RDD[GrapeVertexPartition[VD]]) extends RDD[GrapeVertexPartition[VD]](grapePartitions){
  override def compute(split: Partition, context: TaskContext): Iterator[GrapeVertexPartition[VD]] = {
    val res = firstParent[GrapeVertexPartition[VD]]
    res.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = {
    grapePartitions.partitions
  }

  /**
   * Use this to control the location of partition.
   * @param split
   * @return
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val casted = split.asInstanceOf[GrapeVertexPartition[VD]]
    log.info(s"vertex partition ${casted.pid} preferred ${casted.preferredLocation}")
    Array(casted.preferredLocation)
  }
}
