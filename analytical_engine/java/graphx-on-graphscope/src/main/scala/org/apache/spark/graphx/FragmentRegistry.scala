package org.apache.spark.graphx

import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graphx.{FragmentHolder, SharedMemoryRegistry}
import com.alibaba.graphscope.utils.MappedBuffer
import org.apache.spark.graphx.impl.{GrapeEdgePartition, GrapeUtils}
import org.apache.spark.graphx.traits.{EdgeManager, GraphXVertexIdManager, VertexDataManager}
import org.apache.spark.internal.Logging

import java.io.IOException
import java.net.{InetAddress, UnknownHostException}
import java.util.Objects
import java.util.concurrent.locks.ReentrantLock
import scala.reflect.ClassTag


object FragmentRegistry extends Logging{
  private var maxPartitionId = 0
  private val hostName = GrapeUtils.getSelfHostName
  private var fragId = ""
  private val lock = new ReentrantLock
  private var fragmentHolder = null.asInstanceOf[FragmentHolder]
  //    private static GraphXConf conf;
  private var vertexPartitions = null.asInstanceOf[Array[GrapeVertexPartition[_]]]
  private var edgePartitions = null.asInstanceOf[Array[GrapeEdgePartition[_,_]]]
  private var cnt = 0;

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
      maxPartitionId = Math.max(maxPartitionId, index)
    }

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
	  log.info(s"Fragment rdd has been constructed, partition skip ${fragmentHolder}")
	  lock.unlock();
	  return ;
          //throw new IllegalStateException("Impossible: fragment rdd has been constructed" + fragmentHolder)
        }
        if (fragId == null || fragId.isEmpty){
          throw new IllegalStateException("Please register fragment first")
        }
        fragmentHolder = FragmentHolder.create(fragId, fragName, maxPartitionId + 1)
        val iFragment = fragmentHolder.getIFragment.asInstanceOf[IFragment[Long,Long,_,_]]
        val conf = new GraphXConf[VD,ED]
        val idManager = GraphXFactory.createVertexIdManager[VD,ED](conf, iFragment)
        val vertexDataManager  = GraphXFactory.createVertexDataManager[VD,ED](conf,iFragment,null)
        val edgeManager = GraphXFactory.createEdgeManager(conf, iFragment, idManager, vertexDataManager, numCores)
        log.info(s"create id Manager: ${idManager}")
        log.info(s"create vdata manager: ${vertexDataManager}")
        log.info(s"create edge manager ${edgeManager}")
        log.info("Successfully create fragment RDD")
        //Now create vertexPartitions and edge partitions.
        createVertexPartitions(conf, idManager, vertexDataManager)
        createEdgePartitions(conf, idManager, vertexDataManager,edgeManager)
        lock.unlock()
      }
      else log.info("partition " + pid + " try to get lock failed")
    }
    else log.info("lock has been acquired when partition " + pid + "arrived")
  }

  def createVertexPartitions[VD : ClassTag, ED: ClassTag](conf: GraphXConf[VD, ED], idManager: GraphXVertexIdManager, vertexDataManager: VertexDataManager[VD]): Unit = {
    if (Objects.nonNull(vertexPartitions)) {
      log.error("Recreating vertex partitions is not expected")
      return
    }
    val numPartitions = maxPartitionId + 1
    val chunkSize = (idManager.getInnerVerticesNum + (numPartitions - 1)) / numPartitions
    vertexPartitions = new Array[GrapeVertexPartition[VD]](numPartitions).asInstanceOf[Array[GrapeVertexPartition[_]]]
    for (i <- 0 until numPartitions) {
      val startLid = i * chunkSize;
      val endLid = Math.min(startLid + chunkSize, idManager.getInnerVerticesNum)
      val partitionVdataArray = new Array[VD]((endLid - startLid).toInt)
      var j = 0
      while (j < (endLid - startLid)){
        partitionVdataArray(j) = vertexDataManager.getVertexData(j + startLid)
        j += 1
      }
      vertexPartitions(i) = new GrapeVertexPartition[VD](i, numPartitions, idManager, partitionVdataArray, startLid, endLid)
    }
    log.info("Finish creating javaVertexPartitions of size {}", numPartitions)
  }

  def createEdgePartitions[VD : ClassTag, ED : ClassTag](conf: GraphXConf[VD, ED], idManager: GraphXVertexIdManager, vertexDataManager: VertexDataManager[VD], edgeManager: EdgeManager[VD,ED]): Unit = {
    if (Objects.nonNull(edgePartitions)) {
      log.error("Recreating edge partitions is not expected")
      return
    }

    val numPartitions = maxPartitionId + 1
    edgePartitions = new Array[GrapeEdgePartition[_, _]](numPartitions)
    for (i <- 0 until numPartitions) {
      edgePartitions(i) = (new GrapeEdgePartition[VD, ED](i, numPartitions, idManager, edgeManager))
    }
    log.info("Finish creating javaEdgePartitions of size {}", numPartitions)
  }

  def getVertexPartition[VD: ClassTag](pid: Int): GrapeVertexPartition[VD] = vertexPartitions(pid).asInstanceOf[GrapeVertexPartition[VD]]

  def getEdgePartition[VD : ClassTag, ED : ClassTag](pid: Int): GrapeEdgePartition[VD, ED] = edgePartitions(pid).asInstanceOf[GrapeEdgePartition[VD,ED]]

  def updateVertexPartition[VD : ClassTag](pid : Int, part : GrapeVertexPartition[VD]) : Unit = {
    vertexPartitions(pid) = part
  }


  def mapVertexData[VD : ClassTag](pid : Int, vdataPath : String, size : Long) : Unit = {
    if (!lock.isLocked){
      if (lock.tryLock()){
        log.info(s"Partition ${pid} got lock!")
        if (pid != maxPartitionId) {
          log.info(s"cur pid ${pid}, wait for max pid ${maxPartitionId} to execute")
	  lock.unlock()
          return
        }
        val registry = SharedMemoryRegistry.getOrCreate()
        val buffer = registry.mapFor(vdataPath, size)//Should be created, not reused
        log.info(s"Partition got vdata buffer: ${buffer.remaining()} bytes")
        val vdClass = GrapeUtils.getRuntimeClass[VD].asInstanceOf[Class[VD]]
        val bytesPerEle = GrapeUtils.bytesForType[VD](vdClass)
//        vertexDataManager.writeBackVertexData(buffer)
        val totalLength = vertexPartitions.map(_.values.length).foldLeft(0)(_ + _)
        require(buffer.remaining() >= 8 + bytesPerEle * totalLength, s"size not enough ${buffer.remaining()}, ${8 + bytesPerEle * totalLength}")
 	log.info(s"total length ${totalLength}")
        buffer.writeLong(totalLength)
        if (vdClass.equals(classOf[Long]) || vdClass.equals(classOf[java.lang.Long])){
          log.info(s"partitions size ${vertexPartitions.length}")
          for (partition <- vertexPartitions){
	    log.info(s"partition ${partition}")
            val curVdarray = partition.values
	    val curLength = curVdarray.length
            var i = 0 
            while (i < curLength){
              buffer.writeLong(curVdarray(i).asInstanceOf[Long])
              log.info(s"pid ${pid} write vdata ${curVdarray(i)}")
              i += 1
            }
          }
        }
        else if (vdClass.equals(classOf[Double]) || vdClass.equals(classOf[java.lang.Double])){
          for (partition <- vertexPartitions){
            val curVdarray = partition.values
	    val curLength = curVdarray.length
            var i = 0 
            while (i < curLength){
              buffer.writeDouble(curVdarray(i).asInstanceOf[Double])
              log.info(s"pid ${pid} write vdata ${curVdarray(i)}")
              i += 1
            }
          }
        }
        else if (vdClass.equals(classOf[Int]) || vdClass.equals(classOf[java.lang.Integer])){
          for (partition <- vertexPartitions){
            val curVdarray = partition.values
	    val curLength = curVdarray.length
            var i = 0 
            while (i < curLength){
              buffer.writeInt(curVdarray(i).asInstanceOf[Int])
              log.info(s"pid ${pid} write vdata ${curVdarray(i)}")
              i += 1
            }
          }
        }
        else {
          throw new IllegalStateException("not recognized class:" + vdClass.getName)
        }

        lock.unlock()
        log.info(s"Partition ${pid} release lock")
      }
      else {
        log.info(s"Parttiion ${pid} fails to get lock")
      }
    }
    else {
      log.info(s"Partition ${pid} arrives to write vdata, already locked");
    }
  }
}

