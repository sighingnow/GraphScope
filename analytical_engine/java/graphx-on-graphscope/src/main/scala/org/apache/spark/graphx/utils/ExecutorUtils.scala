package org.apache.spark.graphx.utils

import com.alibaba.graphscope.graphx.VineyardClient
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.PrimitiveVector

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/**
 * Stores info for partitions, executor and hostname. With these info, we can know which
 *
 * - Which partitions are in this executor.
 * - How many partitions are in this executor.
 * - The host name of this executor.
 */
object ExecutorUtils extends Logging{
  private val endPoint = "/tmp/vineyard.sock"
  private val partitionNum = new AtomicInteger(0)
  private val pid2Ind = new mutable.HashMap[Int,Int]
  private val pids = new PrimitiveVector[Int]
  private val hostName: String = InetAddress.getLocalHost.getHostName
  private val hostIp: String = InetAddress.getLocalHost.getHostAddress

  private val vineyardClient: VineyardClient = VineyardClientRegistry.connect(endPoint)
  log.info(s"[GrapeEdgePartitionRegistry]: got vineyard client: ${vineyardClient}")

  def registerPartition(pid : Int) = {
    if (pid2Ind.contains(pid)){
      throw new IllegalStateException(s"Try to register a already registered partition ${pid}")
    }
    pid2Ind(pid) = partitionNum.get()
    pids.+=(pid)
    partitionNum.getAndAdd(1)
  }

  def getPartitionNum: Int = partitionNum.get()

  def print()= {
    log.info(s"On Host [${hostName}/${hostIp}], we have ${getPartitionNum} partitions : ${pids.trim().array.mkString("Array(", ", ", ")")}")
  }

  def getHostName : String = hostName
  def getHostIp : String = hostIp

  def getVineyarClient : VineyardClient = vineyardClient

}
