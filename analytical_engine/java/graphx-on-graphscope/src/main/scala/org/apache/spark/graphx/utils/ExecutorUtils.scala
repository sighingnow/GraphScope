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
  val endPoint = "/tmp/vineyard.sock"
  private val partitionNum = new AtomicInteger(0)
  private val pid2Ind = new mutable.HashMap[Int,Int]
  private val pids = new PrimitiveVector[Int]
  private val hostName: String = InetAddress.getLocalHost.getHostName
  private val hostIp: String = InetAddress.getLocalHost.getHostAddress
  private var localVMID : Long = -1L;
  private var globalVMID : Long = -1L;

  private val vineyardClient: VineyardClient = VineyardClientRegistry.connect(endPoint)
  log.info(s"[ExecutorUtils]: got vineyard client: ${vineyardClient}")

  def registerPartition(pid : Int) = {
    if (pid2Ind.contains(pid)){
      throw new IllegalStateException(s"Try to register a already registered partition ${pid}")
    }
    pid2Ind(pid) = partitionNum.get()
    pids.+=(pid)
    partitionNum.getAndAdd(1)
  }

  def setLocalVMID(vmId : Long) : Unit = {
    require(localVMID == -1, s"vm already been set ${localVMID}")
    this.localVMID = vmId
    log.info(s"[ExecutorUtils]: ${hostName} has local vm id ${this.localVMID}")
  }
  def setGlobalVMID(vmId : Long) : Unit = {
    require(globalVMID == -1, s"vm already been set ${globalVMID}")
    this.globalVMID = vmId
    log.info(s"[ExecutorUtils]: ${hostName} has local vm id ${this.globalVMID}")
  }
  def setGlobalVMIDs(vmIds : java.util.List[String]) : Unit = {
    require(globalVMID == -1, s"vm already been set ${globalVMID}")
    var i = 0
    while (i < vmIds.size()){
      val v = vmIds.get(i)
      if (v.contains(hostName)){
        globalVMID = v.substring(v.indexOf(hostName) + hostName.size + 1).toLong
        log.info(s"Setting global vmID ${globalVMID}")
        i = vmIds.size()
      }
    }
    require(globalVMID != -1)
    log.info(s"[ExecutorUtils]: ${hostName} has local vm id ${this.globalVMID}")
  }

  def getGlobalVMID : Long = {
    require(globalVMID != -1)
    globalVMID
  }

  def getHost2LocalVMID() : String = {
    require(localVMID != -1)
    getHostName + ":" + localVMID
  }

  def getPartitionNum: Int = partitionNum.get()

  def print()= {
    log.info(s"On Host [${hostName}/${hostIp}], we have ${getPartitionNum} partitions : ${pids.trim().array.mkString("Array(", ", ", ")")}")
  }

  def getHostName : String = hostName
  def getHostIp : String = hostIp

  def getVineyarClient : VineyardClient = vineyardClient

}
