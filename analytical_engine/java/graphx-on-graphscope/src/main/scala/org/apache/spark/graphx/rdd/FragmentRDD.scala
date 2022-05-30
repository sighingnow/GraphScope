package org.apache.spark.graphx.rdd

import com.alibaba.fastffi.FFITypeFactory
import com.alibaba.graphscope.fragment.IFragment
import org.apache.spark.graphx.rdd.impl.{FragmentEdgePartition, FragmentEdgeRDDImpl, FragmentVertexPartition, FragmentVertexRDDImpl}
import org.apache.spark.graphx.utils.ScalaFFIFactory
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class FragmentPartition(rddId : Int, override val index : Int, val hostName : String,objectID : Long) extends Partition{

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)

}

class FragmentRDD[VD : ClassTag,ED : ClassTag](sc : SparkContext, val hostNames : Array[String], fragName: String, objectID : Long, socket : String = "/tmp/vineyard.sock")  extends RDD[IFragment[Long,Long,VD,ED]](sc, Nil) with Logging{
  override def compute(split: Partition, context: TaskContext): Iterator[IFragment[Long,Long,VD,ED]] = {
    val client = ScalaFFIFactory.newVineyardClient()
    val ffiByteString = FFITypeFactory.newByteString()
    ffiByteString.copyFrom(socket)
    client.connect(ffiByteString)
    log.info(s"Create vineyard client ${client} and connect to ${socket}")
    val fragment = ScalaFFIFactory.getFragment[VD,ED](client, objectID, fragName)
    log.info(s"Got ifragment ${fragment}")
    Iterator(fragment)
  }

  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](hostNames.length)
    for (i <- 0 until hostNames.length) {
      array(i) = new FragmentPartition(id, i, hostNames(i), objectID)
    }
    array
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Array(split.asInstanceOf[FragmentPartition].hostName)
  }

  def generateVertexRDD() : FragmentVertexRDD[VD] = {
    val partitionRDD = this.mapPartitions(iter => {
      if (iter.hasNext){
        val frag = iter.next()
        Iterator(new FragmentVertexPartition[VD](frag))
      }
      else Iterator.empty
    })
    new FragmentVertexRDDImpl[VD](partitionRDD)
  }

  def generateEdgeRDD() : FragmentEdgeRDD[ED] = {
    val partitionRDD = this.mapPartitions(iter => {
      if (iter.hasNext){
        val frag = iter.next()
        Iterator(new FragmentEdgePartition[VD,ED](frag))
      }
      else Iterator.empty
    })
    new FragmentEdgeRDDImpl[VD,ED](partitionRDD)
  }
}
