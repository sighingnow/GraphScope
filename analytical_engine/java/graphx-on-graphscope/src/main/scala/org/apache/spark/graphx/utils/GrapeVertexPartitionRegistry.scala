package org.apache.spark.graphx.utils

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.graphx.{GrapeVertexPartition, GrapeVertexPartitionBuilder}
import org.apache.spark.graphx.impl.{GrapeUtils, GrapeVertexPartitionWrapper}
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

class GrapeVertexPartitionRegistry[VD : ClassTag] extends Logging{
  val vdClz = GrapeUtils.getRuntimeClass[VD].asInstanceOf[Class[VD]]
  val builderForeignName = "gs::VertexPartitionBuilder<int64_t,uint64_t," + GrapeUtils.classToStr(vdClz) + ">"
  val partitionForeignName = "gs::VertexPartition<int64_t,uint64_t," +  GrapeUtils.classToStr(vdClz) + ">"
  private val builderFactory = FFITypeFactory.getFactory(classOf[GrapeVertexPartitionBuilder[_,_,_]], builderForeignName).asInstanceOf[GrapeVertexPartitionBuilder.Factory[Long,Long,VD]]
  private val partitionFactory = FFITypeFactory.getFactory(classOf[GrapeVertexPartition[_,_,_]], partitionForeignName).asInstanceOf[GrapeVertexPartition.Factory[Long,Long,VD]]
  require(builderFactory != null, s"can not find factory for ${builderForeignName}")
  require(partitionFactory !=null, s"can not find factory for ${partitionForeignName}")
  private var grapeVertexPartition : GrapeVertexPartition[Long,Long,VD] = null.asInstanceOf[GrapeVertexPartition[Long,Long,VD]]
  private var grapeVertexPartitionBuilder: GrapeVertexPartitionBuilder[Long, Long, VD] = null.asInstanceOf[GrapeVertexPartitionBuilder[Long, Long, VD]]

  private val partitionNum : AtomicInteger = new AtomicInteger(0)
  private val partitionCnt : AtomicInteger = new AtomicInteger(0)

  def createVertexPartitionBuilder(pid : Int) : Unit = {
    partitionNum.addAndGet(1)
    if (grapeVertexPartitionBuilder == null){
      synchronized{
        if (grapeVertexPartitionBuilder == null){
          grapeVertexPartitionBuilder = builderFactory.create()
          log.info(s"Partition ${pid} created Builder ${grapeVertexPartitionBuilder}")
          return ;
        }
      }
    }
    log.info(s"Partition ${pid} skip creating builder, part num ${partitionNum.get}")
  }

  def getVertexPartitionBuilder() : GrapeVertexPartitionBuilder[Long,Long,VD] = {
    require(grapeVertexPartitionBuilder != null, "call create first")
    grapeVertexPartitionBuilder
  }

  def build(pid : Int, defaultVal: VD) : Unit = {
    require(grapeVertexPartitionBuilder != null, "builder null")
    if (grapeVertexPartition == null){
      synchronized{
        if (grapeVertexPartition == null){
          grapeVertexPartition = partitionFactory.create()
          log.info(s"Partition ${pid} created partition ${grapeVertexPartitionBuilder}")
          grapeVertexPartitionBuilder.Build(grapeVertexPartition, defaultVal)
          log.info(s"after building partition ${grapeVertexPartition.verticesNum()}")
          return
        }
      }
    }
    log.info(s"Partition ${pid} skip creating vertex Partition")
  }

  def getGrapeVertexPartitionWrapper( pid : Int, numPartitions : Int) : GrapeVertexPartitionWrapper[VD] = {
    require(grapeVertexPartition != null, "grape vertex partitoin null")
    val actualPid = partitionCnt.getAndAdd(1)
    val partInThisProcess = partitionNum.get()
    log.info(s"Partitoin ${pid} try to generate vertex partition wrapper out of ${numPartitions} parts, arrive at ${actualPid}, total part in this executor ${partInThisProcess}")
    val chunkSize = (grapeVertexPartition.verticesNum() + partInThisProcess - 1) / partInThisProcess
    val startLid = chunkSize * actualPid
    val endLid = Math.min(startLid + chunkSize, grapeVertexPartition.verticesNum)
    log.info(s"Partition ${pid}/${numPartitions} got range ${startLid},${endLid}")
    new GrapeVertexPartitionWrapper[VD](pid,numPartitions, startLid, endLid, grapeVertexPartition)
  }
}
object GrapeVertexPartitionRegistry{
  private var registry = null.asInstanceOf[GrapeVertexPartitionRegistry[_]]
  def getOrCreate[VD: ClassTag] : GrapeVertexPartitionRegistry[VD] = {
    if (registry == null){
      synchronized{
        if (registry == null){
          registry = new GrapeVertexPartitionRegistry[VD]
        }
      }
    }
    registry.asInstanceOf[GrapeVertexPartitionRegistry[VD]]
  }

  /**
   * Clear for the next time usages.
   */
  def clear(): Unit ={
    registry = null
  }
}
