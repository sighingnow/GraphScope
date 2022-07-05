package com.alibaba.graphscope.graphx.rdd

import com.alibaba.fastffi.{FFIByteString, FFITypeFactory}
import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.rdd.FragmentPartition.getHost
import com.alibaba.graphscope.graphx.rdd.VineyardPartition.socket
import com.alibaba.graphscope.graphx.utils.ScalaFFIFactory
import org.apache.spark.internal.Logging
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import java.net.InetAddress

class VineyardPartition(val ind : Int,val hostName : String) extends Partition with Logging{
  lazy val client: VineyardClient = {
    if (hostName.equals(InetAddress.getLocalHost.getHostName)) {
      val res = ScalaFFIFactory.newVineyardClient()
      val ffiByteString: FFIByteString = FFITypeFactory.newByteString()
      ffiByteString.copyFrom(socket)
      res.connect(ffiByteString)
      log.info(s"successfully connect to ${socket}")
      res
    }
    else {
      log.info(s"This partition should be evaluated on this host since it is not on the desired host,desired host ${hostName}, cur host ${getHost}")
      null
    }
  }
  override def index: Int = ind
}
object VineyardPartition{
  val socket = "/tmp/vineyard.sock"
}

class VineyardRDD(sc : SparkContext, val locations : Array[String], val hostNames : Array[String]) extends RDD[VineyardClient](sc, Nil){
  val vineyardParts = new Array[VineyardPartition](locations.length)
  for  (i <- vineyardParts.indices){
    vineyardParts(i) = new VineyardPartition(i,hostNames(i))
  }
  override def compute(split: Partition, context: TaskContext): Iterator[VineyardClient] = {
    val casted = split.asInstanceOf[VineyardPartition]
    Iterator(casted.client)
  }

  override protected def getPartitions: Array[Partition] = {
    vineyardParts.asInstanceOf[Array[Partition]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val casted = split.asInstanceOf[VineyardPartition]
    log.info(s"get pref location for ${casted.hostName} ${casted.ind}")
    Array(locations(casted.ind))
  }
}
