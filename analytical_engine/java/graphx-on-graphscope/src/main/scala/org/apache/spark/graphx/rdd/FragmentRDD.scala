package org.apache.spark.graphx.rdd

import com.alibaba.fastffi.{FFIByteString, FFITypeFactory}
import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.graph.impl.FragmentStructure
import org.apache.spark.graphx.impl.grape.GrapeEdgeRDDImpl
import org.apache.spark.graphx.impl.partition.GrapeEdgePartition
import org.apache.spark.graphx.utils.ScalaFFIFactory
import org.apache.spark.graphx.{GrapeEdgeRDD, GrapeVertexRDD, PartitionID}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class FragmentPartition[VD : ClassTag,ED : ClassTag](rddId : Int, override val index : Int, val hostName : String,objectID : Long, socket : String, fragName : String) extends Partition with Logging {

  val client: VineyardClient = ScalaFFIFactory.newVineyardClient()
  val ffiByteString: FFIByteString = FFITypeFactory.newByteString()
  ffiByteString.copyFrom(socket)
  client.connect(ffiByteString)
  log.info(s"Create vineyard client ${client} and connect to ${socket}")
  val fragment: IFragment[Long, Long, VD, ED] = ScalaFFIFactory.getFragment[VD,ED](client, objectID, fragName)
  log.info(s"Got iFragment ${fragment}")
  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)

}

class FragmentRDD[VD : ClassTag,ED : ClassTag](sc : SparkContext, val hostNames : Array[String], fragName: String, objectID : Long, socket : String = "/tmp/vineyard.sock")  extends RDD[(PartitionID,(VineyardClient,IFragment[Long,Long,VD,ED]))](sc, Nil) with Logging{
  override def compute(split: Partition, context: TaskContext): Iterator[(PartitionID,(VineyardClient,IFragment[Long,Long,VD,ED]))] = {
    val partitionCasted = split.asInstanceOf[FragmentPartition[VD,ED]]
    Iterator((partitionCasted.index, (partitionCasted.client, partitionCasted.fragment)))
  }

  /** according to spark code comments, this function will be only executed once. */
  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](hostNames.length)
    for (i <- 0 until hostNames.length) {
      array(i) = new FragmentPartition(id, i, hostNames(i), objectID, socket)
    }
    array
  }

  /**
   * Use this to control the location of partition.
   * @param split
   * @return
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Array(split.asInstanceOf[FragmentPartition].hostName)
  }

  def generateRDD() : (GrapeVertexRDD[VD],GrapeEdgeRDD[ED]) = {
    val fragmentStructures = this.mapPartitions(iter => {
      if (iter.hasNext){
        val (pid, (client,frag)) = iter.next()
        Iterator(new FragmentStructure(frag))
      }
      else Iterator.empty
    }).cache()
    val pid2Fids = this.mapPartitions(iter => {
      val tuple = iter.next()
      Iterator((tuple._1, tuple._2._2.fid()))
    }).collect()
    log.info(s"pid2fids ${pid2Fids.mkString("Array(", ", ", ")")}")
    fragmentStructures.foreachPartition(iter => {
      val fragmentStructure = iter.next()
      fragmentStructure.initFid2GraphxPid(pid2Fids)
    })
    val edgePartitions = this.zipPartitions(fragmentStructures){
      (fragIter, structureIter) => {
        if (fragIter.hasNext){
          val (pid,(client,frag)) = fragIter.next()
          val structure = structureIter.next()
          Iterator(new GrapeEdgePartition[VD,ED](pid, structure, client, null))
        }
        else Iterator.empty
      }
    }.cache()
    val edgeRDD = new GrapeEdgeRDDImpl[VD,ED](edgePartitions)

    val vertexRDD = GrapeVertexRDD.fromFragmentEdgeRDD[VD](edgeRDD, edgePartitions.getNumPartitions)
    (vertexRDD,edgeRDD)
  }
}
