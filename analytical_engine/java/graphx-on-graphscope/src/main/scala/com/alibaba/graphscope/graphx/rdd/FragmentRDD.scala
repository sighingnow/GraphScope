package com.alibaba.graphscope.graphx.rdd

import com.alibaba.fastffi.{FFIByteString, FFITypeFactory}
import com.alibaba.graphscope.ds.TypedArray
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor
import com.alibaba.graphscope.fragment.{ArrowProjectedFragment, FragmentType, IFragment}
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.graph.impl.FragmentStructure
import com.alibaba.graphscope.graphx.rdd.FragmentPartition.getHost
import com.alibaba.graphscope.graphx.rdd.impl.GrapeEdgePartition
import com.alibaba.graphscope.graphx.store.{AbstractInHeapDataStore, InHeapEdgeDataStore}
import com.alibaba.graphscope.graphx.utils.{ArrayWithOffset, EIDAccessor, ScalaFFIFactory}
import org.apache.spark.graphx.PartitionID
import org.apache.spark.graphx.grape.{GrapeEdgeRDD, GrapeVertexRDD, PartitionAwareZippedBaseRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import java.net.InetAddress
import scala.collection.mutable
import scala.reflect.ClassTag

class FragmentPartition[VD : ClassTag,ED : ClassTag](rddId : Int, override val index : Int, val hostName : String,val executorId : String,objectID : Long, socket : String, fragName : String) extends Partition with Logging {

  /** mark this val as lazy to let it run on executor rather than driver */
  lazy val tuple = {
    if (hostName.equals(getHost)){
      val client: VineyardClient = ScalaFFIFactory.newVineyardClient()
      val ffiByteString: FFIByteString = FFITypeFactory.newByteString()
      ffiByteString.copyFrom(socket)
      client.connect(ffiByteString)
      log.info(s"Create vineyard client ${client} and connect to ${socket}")
      val fragment: IFragment[Long, Long, VD, ED] = ScalaFFIFactory.getFragment[VD,ED](client, objectID, fragName)
      log.info(s"Got iFragment ${fragment}")
      (client, fragment)
    }
    else {
      log.info(s"This partition should be evaluated on this host since it is not on the desired host,desired host ${hostName}, cur host ${getHost}")
      null
    }
  }

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)

}
object FragmentPartition{
  def getHost : String = {
    InetAddress.getLocalHost.getHostName
  }
}

class FragmentRDD[VD : ClassTag,ED : ClassTag](sc : SparkContext, executorId2Host : mutable.HashMap[String,String], fragName: String, objectIDs : String, socket : String = "/tmp/vineyard.sock")  extends RDD[(PartitionID,(VineyardClient,IFragment[Long,Long,VD,ED]))](sc, Nil) with Logging{
  //objectIds be like d50:id1,d51:id2
  val objectsSplited: Array[String] = objectIDs.split(",")
  val map: mutable.Map[String,Long] = mutable.Map[String, Long]()
  require(objectsSplited.length == executorId2Host.size, s"executor's host names length not equal to object ids ${executorId2Host.toArray.mkString("Array(", ", ", ")")}, ${objectIDs}")
  for (str <- objectsSplited){
    val hostAndId = str.split(":")
    require(hostAndId.length == 2)
    val host = hostAndId(0)
    val id = hostAndId(1)
    require(!map.contains(host), s"entry for host ${host} already set ${map.get(host)}")
    map(host) = id.toLong
    log.info(s"host ${host}: objId : ${id}")
  }


  override def compute(split: Partition, context: TaskContext): Iterator[(PartitionID,(VineyardClient,IFragment[Long,Long,VD,ED]))] = {
    val partitionCasted = split.asInstanceOf[FragmentPartition[VD,ED]]
    Iterator((partitionCasted.index, partitionCasted.tuple))
  }

  /** according to spark code comments, this function will be only executed once. */
  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](objectsSplited.length)
    val iter = executorId2Host.iterator
    for (i <- array.indices) {
      val (executorId, executorHost) = iter.next()
      log.info(s"executorId ${executorId}, host ${executorHost}, corresponding obj id ${map(executorHost)}")
      array(i) = new FragmentPartition[VD,ED](id, i, executorHost, executorId,map(executorHost), socket,fragName)
    }
    array
  }

  /**
   * Use this to control the location of partition.
   * @param split
   * @return
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val casted = split.asInstanceOf[FragmentPartition[VD,ED]]
    //TaskLocation.executorTag = executor_
    val location = "executor_" + casted.hostName + "_" + casted.executorId
    log.info(s"get pref location for ${casted.hostName} ${location}")
    Array(location)
  }

  def generateRDD() : (GrapeVertexRDD[VD],GrapeEdgeRDD[ED]) = {
    this.cache()
    val fragmentStructures = this.mapPartitions(iter => {
      if (iter.hasNext){
        val (pid, (client,frag)) = iter.next()
        Iterator(new FragmentStructure(frag))
      }
      else Iterator.empty
    },preservesPartitioning = true).cache()
    val pid2Fids = this.mapPartitions(iter => {
      val tuple = iter.next()
      Iterator((tuple._1, tuple._2._2.fid()))
    },preservesPartitioning = true).collect()
    log.info(s"pid2fids ${pid2Fids.mkString("Array(", ", ", ")")}")
    fragmentStructures.foreachPartition(iter => {
      val fragmentStructure = iter.next()
      fragmentStructure.initFid2GraphxPid(pid2Fids)
    })

      val edgePartitions = PartitionAwareZippedBaseRDD.zipPartitions(SparkContext.getOrCreate(), this, fragmentStructures){
      (fragIter, structureIter) => {
        if (fragIter.hasNext){
          val (pid,(client,frag)) = fragIter.next()
          val structure = structureIter.next()
          val time0 = System.nanoTime()
          val newEdata = new Array[ED](structure.getTotalEdgesNum.toInt)
//          val eids = structure.eids
          val eidAccessor = new EIDAccessor(0)
          if (frag.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
            val projectedFragment = frag.asInstanceOf[ArrowProjectedAdaptor[Long, Long, _, _]].getArrowProjectedFragment.asInstanceOf[ArrowProjectedFragment[Long,Long,_,_]]
            val edataAccessor = projectedFragment.getEdataArrayAccessor.asInstanceOf[TypedArray[ED]]
            var i = 0
            val len = structure.getTotalEdgesNum.toInt
            while (i < len){
              newEdata(i) = edataAccessor.get(i)
              i += 1
            }
            eidAccessor.address = projectedFragment.getOutEdgesPtr.getAddress
          }
          val time1 = System.nanoTime()
          val edgeStore = new InHeapEdgeDataStore[ED](newEdata.length,client,1, newEdata, eidAccessor)
          log.info(s"got edata array cost ${(time1 - time0)/ 1000000}ms")
          val siblingPids = new Array[Int](1)
          siblingPids(0) = pid
          Iterator(new GrapeEdgePartition[VD,ED](pid,0,1, siblingPids,0,frag.getInnerVerticesNum, structure.getOutEdgesNum.toInt, structure, client, edgeStore))
        }
        else Iterator.empty
      }
    }.cache()
    val edgeRDD = GrapeEdgeRDD.fromPartitions(edgePartitions)

    val vertexRDD = GrapeVertexRDD.fromFragmentEdgeRDD[VD](edgeRDD, edgePartitions.getNumPartitions)
    (vertexRDD,edgeRDD)
  }
}
