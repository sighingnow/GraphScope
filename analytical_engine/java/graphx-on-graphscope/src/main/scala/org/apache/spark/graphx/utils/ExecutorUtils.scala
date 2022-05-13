package org.apache.spark.graphx.utils

import com.alibaba.graphscope.graphx.{GraphXCSR, GraphXVertexMap, VineyardClient}
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
  val endPoint = Constant.vineyardEndpoint
  private val partitionNum = new AtomicInteger(0)
  private val pid2Ind = new mutable.HashMap[Int,Int]
  private val pids = new PrimitiveVector[Int]
  private val hostName: String = InetAddress.getLocalHost.getHostName
  private val hostIp: String = InetAddress.getLocalHost.getHostAddress
  private var localVMID : Long = -1L;
  private var csrID : Long = -1L;
  private var globalVMID : Long = -1L;
  private var graphXCSR : GraphXCSR[Long,_] = null.asInstanceOf[GraphXCSR[Long,_]]
  private var graphXVertexMap : GraphXVertexMap[Long,Long] = null.asInstanceOf[GraphXVertexMap[Long,Long]]

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

  def graphXPid2GrapePid(pid : Int) : Int = {
    if (!pid2Ind.contains(pid)){
      throw new IllegalStateException(s"try to get grape pid for a not registered graphx partition: ${pid}")
    }
    pid2Ind.get(pid).get
  }

  def setLocalVMID(vmId : Long) : Unit = {
    require(this.localVMID == -1, s"vm already been set ${localVMID}")
    this.localVMID = vmId
    log.info(s"[ExecutorUtils]: ${hostName} has local vm id ${this.localVMID}")
  }
  def setCSRID(csrId : Long) : Unit = {
    require(this.csrID == -1, s"vm already been set ${localVMID}")
    this.csrID = csrId
    log.info(s"[ExecutorUtils]: ${hostName} set csr id ${this.csrID}")
  }
  def setGraphXCSR(csr : GraphXCSR[Long,_]) : Unit = {
    require(this.graphXCSR == null)
    this.graphXCSR = csr
    log.info(s"[ExecutorUtils]: ${hostName} set csr to ${this.graphXCSR}")
  }
  def getGraphXCSR : GraphXCSR[Long,_] = {
    require(graphXCSR != null)
    graphXCSR
  }

  def setGlobalVMID(vmId : Long) : Unit = {
    require(this.globalVMID == -1, s"vm already been set ${globalVMID}")
    this.globalVMID = vmId
    log.info(s"[ExecutorUtils]: ${hostName} has local vm id ${this.globalVMID}")
  }
  def setGlobalVMIDs(vmIds : java.util.List[String]) : Unit = {
    synchronized {
      val res = filterHostGlobalVMIDs(vmIds)
      if (this.globalVMID == -1L){
        this.globalVMID = res
        log.info(s"[ExecutorUtils]: ${hostName} set global vm id ${this.globalVMID}")
      }
      else {
        require(this.globalVMID == res, "two different set for global vm id")
        log.info(s"[ExecutorUtils]: ${hostName} already set global vm id to ${this.globalVMID}, this set is same with previous")
      }
      require(globalVMID != -1)
    }
  }

  def filterHostGlobalVMIDs(vmIds : java.util.List[String]) : Long = {
    var i = 0
    while (i < vmIds.size()){
      val v = vmIds.get(i)
      if (v.contains(hostName)){
        globalVMID = v.substring(v.indexOf(hostName) + hostName.size + 1).toLong
        log.info(s"parsed global vmID ${globalVMID}")
        return globalVMID
      }
      i += 1
    }
    throw new IllegalStateException(s"No global vm id parsed out from ${vmIds}")
  }

  def setGlobalVM(vm : GraphXVertexMap[Long,Long]) : Unit = {
    require(this.graphXVertexMap == null)
    this.graphXVertexMap = vm
    log.info(s"[ExecutorUtils]: ${hostName} set glbal vm to ${this.graphXVertexMap}")
  }
  def getGlobalVM : GraphXVertexMap[Long,Long] = {
    require(graphXVertexMap != null)
    graphXVertexMap
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
