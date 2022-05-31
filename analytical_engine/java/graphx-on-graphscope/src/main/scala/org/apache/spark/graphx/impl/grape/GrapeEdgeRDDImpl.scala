package org.apache.spark.graphx.impl.grape

import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.EdgeRDDImpl
import org.apache.spark.graphx.impl.partition.GrapeEdgePartition
import org.apache.spark.graphx.impl.partition.data.{InHeapVertexDataStore, VertexDataStore}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, Partitioner, TaskContext, graphx}

import scala.reflect.{ClassTag, classTag}

class GrapeEdgeRDDImpl [VD: ClassTag, ED: ClassTag] private[graphx](@transient override val grapePartitionsRDD: RDD[GrapeEdgePartition[VD, ED]],
                                                                     val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends GrapeEdgeRDD[ED](grapePartitionsRDD.context, List(new OneToOneDependency(grapePartitionsRDD))) {

  override protected def getPartitions: Array[Partition] = grapePartitionsRDD.partitions

  override val partitioner: Option[Partitioner] = grapePartitionsRDD.partitioner

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    val p = firstParent[(PartitionID, GrapeEdgePartition[VD, ED])].iterator(part, context)
    if (p.hasNext) {
      p.next()._2.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }

  override def setName(_name: String): this.type = {
    if (grapePartitionsRDD.name != null) {
      grapePartitionsRDD.setName(grapePartitionsRDD.name + ", " + _name)
    } else {
      grapePartitionsRDD.setName(_name)
    }
    this
  }
  setName("GrapeEdgeRDDImpl")

  override def generateDegreeRDD(originalVertexRDD : GrapeVertexRDD[_],
                                 edgeDirection: EdgeDirection) : GrapeVertexRDD[Int] = {
//    val grapeVertexRDDImpl = originalVertexRDD.asInstanceOf[GrapeVertexRDDImpl[_]]
    val newVertexPartitionRDD = this.grapePartitionsRDD.zipPartitions(originalVertexRDD.grapePartitionsRDD, true){
      (thisIter, otherIter) => {
        val ePart = thisIter.next()
        val otherVPart = otherIter.next()
        //VertexPartition id range should be same with edge partition
        val newVdArray = ePart.getDegreeArray(edgeDirection)
        require(otherVPart.vertexData.size == newVdArray.size())
        val newVPart = otherVPart.withNewValues(new InHeapVertexDataStore[Int](newVdArray,otherVPart.client))
        Iterator(newVPart)
      }
    }
    log.info(s"get degree rdd with direction ${edgeDirection}")
    originalVertexRDD.withGrapePartitionsRDD(newVertexPartitionRDD)
  }

  override def collect(): Array[Edge[ED]] = this.map(_.copy()).collect()

  override def persist(newLevel: StorageLevel): this.type = {
    grapePartitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = false): this.type = {
    grapePartitionsRDD.unpersist(blocking)
    this
  }

  override def cache(): this.type = {
    grapePartitionsRDD.persist(targetStorageLevel)
    this
  }

  override def getStorageLevel: StorageLevel = grapePartitionsRDD.getStorageLevel

  override def checkpoint(): Unit = {
    grapePartitionsRDD.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    firstParent[(PartitionID, GrapeEdgePartition[VD, ED])].isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    grapePartitionsRDD.getCheckpointFile
  }

  //FIXME: count active edges
  override def count(): Long = {
    grapePartitionsRDD.map(_.activeEdgeSet.cardinality()).fold(0)(_ + _)
  }

  override def mapValues[ED2 :ClassTag](f: Edge[ED] => ED2): GrapeEdgeRDDImpl[VD,ED2] = {
    mapEdgePartitions((part) => part.map(f))
  }

  override def reverse: GrapeEdgeRDD[ED] = {
    mapEdgePartitions((partition) => partition.reverse)
  }

//  def filter(
//              epred: EdgeTriplet[VD, ED] => Boolean,
//              vpred: (VertexId, VD) => Boolean,
//              vertexDataStore: VertexDataStore[VD]): GrapeEdgeRDDImpl[VD, ED] = {
//    mapEdgePartitions((pid, part) => part.filter(epred, vpred, vertexDataStore))
//  }
  override def innerJoin[ED2: ClassTag, ED3: ClassTag]
  (other: EdgeRDD[ED2])
  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgeRDD[ED3] = {
    val newPartitions = grapePartitionsRDD.zipPartitions(other.asInstanceOf[graphx.GrapeEdgeRDD[ED2]].grapePartitionsRDD, true)({
      (thisIter, otherIter) => {
        val thisEpart = thisIter.next()
        val  otherEpart = otherIter.next()
        Iterator(thisEpart.innerJoin(otherEpart)(f))
      }
    })
    this.withPartitionsRDD[VD,ED3](newPartitions)
  }

  override private[graphx] def withTargetStorageLevel(newTargetStorageLevel: StorageLevel) : GrapeEdgeRDDImpl[VD,ED] = {
    new GrapeEdgeRDDImpl[VD,ED](grapePartitionsRDD, newTargetStorageLevel)
  }

  def mapEdgePartitions[VD2: ClassTag, ED2: ClassTag](
           f: GrapeEdgePartition[VD, ED] => GrapeEdgePartition[VD2, ED2]): GrapeEdgeRDDImpl[VD2, ED2] = {
    this.withPartitionsRDD[VD2, ED2](grapePartitionsRDD.mapPartitions({ iter =>
      if (iter.hasNext) {
        val ep = iter.next()
        Iterator(f(ep))
      } else {
        Iterator.empty
      }
    }, preservesPartitioning = true))
  }

  def withPartitionsRDD[VD2: ClassTag, ED2: ClassTag](
          partitionsRDD: RDD[GrapeEdgePartition[VD2, ED2]]): GrapeEdgeRDDImpl[VD2, ED2] = {
    new GrapeEdgeRDDImpl[VD2,ED2](partitionsRDD, this.targetStorageLevel)
  }
}
