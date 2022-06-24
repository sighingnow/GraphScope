package org.apache.spark.graphx.utils

import org.apache.spark.internal.Logging

import java.net.InetAddress

/**
 * Stores info for partitions, executor and hostname. With these info, we can know which
 *
 * - Which partitions are in this executor.
 * - How many partitions are in this executor.
 * - The host name of this executor.
 */
object ExecutorUtils extends Logging{

  private val hostName: String = InetAddress.getLocalHost.getHostName
  private val hostIp: String = InetAddress.getLocalHost.getHostAddress
  val vineyardEndpoint = "/tmp/vineyard.sock"

  def getHostName : String = hostName
  def getHostIp : String = hostIp

}
