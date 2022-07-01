package com.alibaba.graphscope.graphx.utils

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
  val vineyardEndpoint = "/tmp/vineyard.sock"

  def getHostName : String = InetAddress.getLocalHost.getHostName
  def getHostIp : String = InetAddress.getLocalHost.getHostAddress

}
