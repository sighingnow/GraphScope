package org.apache.spark.graphx

import com.alibaba.graphscope.conf.GraphXConf
import com.alibaba.graphscope.factory.GraphXFactory
import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graph.{GraphXVertexIdManager, VertexDataManager}
import com.alibaba.graphscope.graphx.FragmentHolder
import org.apache.spark.graphx.impl.GrapeEdgePartition
import org.apache.spark.internal.Logging

import java.io.IOException
import java.net.{InetAddress, UnknownHostException}
import java.util.Objects
import java.util.concurrent.locks.ReentrantLock
import scala.reflect.ClassTag


object FragmentRegistry extends Logging{
  private var maxPartitionId = 0
  private val hostName = getSelfHostName
  private var fragId = ""
  private val lock = new ReentrantLock
  private var fragmentHolder = null.asInstanceOf[FragmentHolder]
  //    private static GraphXConf conf;
  private var vertexPartitions = null.asInstanceOf[Array[GrapeVertexPartition[_]]]
  private var edgePartitions = null.asInstanceOf[Array[GrapeEdgePartition[_,_]]]

  def initConf[VD, ED](vdClass: Class[_ <: VD], edClass: Class[_ <: ED]): GraphXConf[VD, ED, _] = {
    val conf = new GraphXConf[VD, ED, AnyRef]
    if (vdClass == classOf[Long] || vdClass == classOf[Long]) conf.setVdataClass(classOf[Long].asInstanceOf[Class[_ <: VD]])
    else if (vdClass == classOf[Int] || vdClass == classOf[Integer]) conf.setVdataClass(classOf[Integer].asInstanceOf[Class[_ <: VD]])
    else if (vdClass == classOf[Double] || vdClass == classOf[Double]) conf.setVdataClass(classOf[Double].asInstanceOf[Class[_ <: VD]])
    else throw new IllegalStateException("Error vd class: " + vdClass.getName)
    if (edClass == classOf[Long] || edClass == classOf[Long]) conf.setEdataClass(classOf[Long].asInstanceOf[Class[_ <: ED]])
    else if (edClass == classOf[Int] || edClass == classOf[Integer]) conf.setEdataClass(classOf[Integer].asInstanceOf[Class[_ <: ED]])
    else if (edClass == classOf[Double] || edClass == classOf[Double]) conf.setEdataClass(classOf[Double].asInstanceOf[Class[_ <: ED]])
    else throw new IllegalStateException("Error ed class: " + edClass.getName)
    conf
  }

  @throws[IOException]
  def registFragment(fragIds: String, index: Int): Int = {
    this.synchronized{
      val host2frag = fragIds.split(",")
      if (fragId.isEmpty){
        for (item <- host2frag) {
          log.info("test: " + item + " start with " + hostName + ", matches: " + item.startsWith(hostName))
          if (item.startsWith(hostName)) {
            FragmentRegistry.fragId = item.split(":")(1)
            log.info("on host " + hostName + " get frag id " + fragId + "\n")
          }
        }
      }
    }

    maxPartitionId = Math.max(maxPartitionId, index)
    log.info("max Partition id: " + maxPartitionId)
    index
  }

  /**
   * For each partition/thread, this function should only run once.
   */
  @throws[IOException]
  def constructFragment[VD : ClassTag, ED : ClassTag](pid: Int, fragName: String, vdClass: Class[VD], edClass: Class[ED], numCores: Int): Unit = {
    if (!lock.isLocked){
      if (lock.tryLock) {
        log.info("partition " + pid + " successfully got lock")
        if (fragmentHolder != null){
          throw new IllegalStateException("Impossible: fragment rdd has been constructed" + fragmentHolder)
        }
        if (fragId == null || fragId.isEmpty){
          throw new IllegalStateException("Please register fragment first")
        }
        fragmentHolder = FragmentHolder.create(fragId, fragName, maxPartitionId + 1)
        val conf = initConf[VD,ED](vdClass, edClass).asInstanceOf[GraphXConf[VD,ED,_]]
        val idManager = GraphXFactory.createIdManager(conf)
        val vertexDataManager  = GraphXFactory.createVertexDataManagerv2[VD,ED](conf)
        idManager.init(fragmentHolder.getIFragment.asInstanceOf[IFragment[java.lang.Long,java.lang.Long,_,_]])
        vertexDataManager.init(fragmentHolder.getIFragment.asInstanceOf[IFragment[java.lang.Long,java.lang.Long,VD,_]])
        log.info("create id Manager: {}", idManager)
        log.info("create vdata manager: {}", vertexDataManager)
        log.info("Successfully create fragment RDD")
        //Now create vertexPartitions and edge partitions.
        createVertexPartitions(conf, idManager, vertexDataManager)
        createEdgePartitions(conf, idManager, vertexDataManager, numCores)
        lock.unlock()
      }
      else log.info("partition " + pid + " try to get lock failed")
    }
    else log.info("lock has been acquired when partition " + pid + "arrived")
  }

  def createVertexPartitions[VD : ClassTag, ED: ClassTag](conf: GraphXConf[VD, ED, _], idManager: GraphXVertexIdManager, vertexDataManager: VertexDataManager[VD]): Unit = {
    if (Objects.nonNull(vertexPartitions)) {
      log.error("Recreating vertex partitions is not expected")
      return
    }
    val numPartitions = maxPartitionId + 1
    vertexPartitions = new Array[GrapeVertexPartition[VD]](numPartitions).asInstanceOf[Array[GrapeVertexPartition[_]]]
    for (i <- 0 until numPartitions) {
      vertexPartitions(i) = new GrapeVertexPartition[VD](i, numPartitions, idManager, vertexDataManager)
    }
    log.info("Finish creating javaVertexPartitions of size {}", numPartitions)
  }

  def createEdgePartitions[VD : ClassTag, ED : ClassTag](conf: GraphXConf[VD, ED, _], idManager: GraphXVertexIdManager, vertexDataManager: VertexDataManager[VD], numCores: Int): Unit = {
    if (Objects.nonNull(edgePartitions)) {
      log.error("Recreating edge partitions is not expected")
      return
    }
    val edgeManager = GraphXFactory.createEdgeManager(conf, idManager, vertexDataManager)
    edgeManager.init(fragmentHolder.getIFragment.asInstanceOf[IFragment[java.lang.Long,java.lang.Long,VD,ED]], numCores)
    val numPartitions = maxPartitionId + 1
    edgePartitions = new Array[GrapeEdgePartition[_, _]](numPartitions)
    for (i <- 0 until numPartitions) {
      edgePartitions(i) = (new GrapeEdgePartition[VD, ED](i, numPartitions, idManager, edgeManager))
    }
    log.info("Finish creating javaEdgePartitions of size {}", numPartitions)
  }

  def getVertexPartition[VD: ClassTag](pid: Int): GrapeVertexPartition[VD] = vertexPartitions(pid).asInstanceOf[GrapeVertexPartition[VD]]

  def getEdgePartition[VD : ClassTag, ED : ClassTag](pid: Int): GrapeEdgePartition[VD, ED] = edgePartitions(pid).asInstanceOf[GrapeEdgePartition[VD,ED]]

  @throws[UnknownHostException]
  private def getSelfHostName = InetAddress.getLocalHost.getHostName
}

