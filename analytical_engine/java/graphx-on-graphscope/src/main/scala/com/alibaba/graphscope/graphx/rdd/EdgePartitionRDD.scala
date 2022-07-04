package com.alibaba.graphscope.graphx.rdd

import com.alibaba.graphscope.graphx.rdd.impl.GrapeEdgePartition
import org.apache.spark.rdd.RDD
import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class EdgePartitionRDD[VD : ClassTag, ED : ClassTag](sc : SparkContext, val grapePartitions : RDD[GrapeEdgePartition[VD,ED]]) extends RDD[GrapeEdgePartition[VD,ED]](grapePartitions){
  override def compute(split: Partition, context: TaskContext): Iterator[GrapeEdgePartition[VD,ED]] = {
    val res = firstParent[GrapeEdgePartition[VD,ED]]
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
    val casted = split.asInstanceOf[GrapeEdgePartition[VD,ED]]
    log.info(s"edge partition ${casted.pid} preferred ${casted.preferredLocation}")
    Array(casted.preferredLocation)
  }
}
