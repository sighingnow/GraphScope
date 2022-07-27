package com.alibaba.graphscope.graphx.rdd

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

class EmptyPartition(val ind : Int,val hostName : String, val loc : String) extends Partition with Logging{
  override def index: Int = ind
}

class LocationAwareRDD(sc : SparkContext, val locations : Array[String], val hostNames : Array[String],val partitionsIds : Array[Int]) extends RDD[EmptyPartition](sc, Nil){
  val parts = new Array[EmptyPartition](locations.length)
  for  (i <- parts.indices){
    val ind = partitionsIds(i)
    parts(ind) = new EmptyPartition(ind,hostNames(i),locations(i))
  }
  override def compute(split: Partition, context: TaskContext): Iterator[EmptyPartition] = {
    val casted = split.asInstanceOf[EmptyPartition]
    Iterator(casted)
  }

  override protected def getPartitions: Array[Partition] = {
    parts.asInstanceOf[Array[Partition]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val casted = split.asInstanceOf[EmptyPartition]
//    log.info(s"get pref location for ${casted.ind}, ${casted.loc}")
    Array(casted.loc)
  }
}
