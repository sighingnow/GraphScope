package org.apache.spark.graphx.impl.grape

import org.apache.spark.graphx.impl.GrapeEdgePartition
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, Partitioner, TaskContext}

import scala.reflect.{ClassTag, classTag}

class GrapeEdgeRDDImpl [VD: ClassTag, ED: ClassTag] private[graphx](
                                                                        @transient val grapePartitionsRDD: RDD[(PartitionID, GrapeEdgePartition[VD, ED])],
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
  setName("OffHeapGrapeEdgeRDD")

  override def generateDegreeRDD(originalVertexRDD : GrapeVertexRDD[_]) : GrapeVertexRDD[Int] = {
    val grapeVertexRDDImpl = originalVertexRDD.asInstanceOf[GrapeVertexRDDImpl[_]]
    val newVertexPartitionRDD = this.grapePartitionsRDD.zipPartitions(grapeVertexRDDImpl.grapePartitionsRDD, true){
      (thisIter, otherIter) => {
        val (pid, thisEPart) = thisIter.next()
        val (_, otherVPart) = otherIter.next()
//        val newPart = thisEPart.innerJoin[ED2, ED3](otherEPart)(f)
        val newVPart = otherVPart.withNewValues(thisEPart.getDegreeArray(otherVPart.startLid, otherVPart.endLid))
        Iterator((pid, newVPart))
      }
    }
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
    grapePartitionsRDD.map(_._2.numEdges.toLong).fold(0)(_ + _)
  }

  override def mapValues[ED2 :ClassTag](f: Edge[ED] => ED2): GrapeEdgeRDDImpl[VD,ED2] = {
    mapEdgePartitions((pid, part) => part.map(f))
  }

  override def reverse: EdgeRDD[ED] = {
    mapEdgePartitions((pid, partition) => partition.reverse)
  }

  def filter(
              epred: EdgeTriplet[VD, ED] => Boolean,
              vpred: (VertexId, VD) => Boolean): GrapeEdgeRDDImpl[VD, ED] = {
    mapEdgePartitions((pid, part) => part.filter(epred, vpred))
  }

  override def innerJoin[ED2: ClassTag, ED3: ClassTag](other: EdgeRDD[ED2])(f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgeRDD[ED3] = {
    other match {
      case other : GrapeEdgeRDDImpl[VD,ED2] if this.partitioner == other.partitioner => {
        val ed2Tag = classTag[ED2]
        val ed3Tag = classTag[ED3]
        this.withPartitionsRDD[VD, ED3](
          grapePartitionsRDD.zipPartitions(other.grapePartitionsRDD, true) {
            (thisIter, otherIter) =>
              val (pid, thisEPart) = thisIter.next()
              val (_, otherEPart) = otherIter.next()
              // Iterator((pid, thisEPart.innerJoin(otherEPart)(f)(ed2Tag, ed3Tag)))
              val newPart = thisEPart.innerJoin[ED2, ED3](otherEPart)(f)
              Iterator((pid, newPart))
          })
      }
    }
  }

  override private[graphx] def withTargetStorageLevel(newTargetStorageLevel: StorageLevel) = {
    new GrapeEdgeRDDImpl[VD,ED](grapePartitionsRDD, newTargetStorageLevel)
  }

  override private[graphx] def partitionsRDD = {
    throw new IllegalStateException("Not implemented")
  }

  def mapEdgePartitions[VD2: ClassTag, ED2: ClassTag](
           f: (PartitionID, GrapeEdgePartition[VD, ED]) => GrapeEdgePartition[VD2, ED2]): GrapeEdgeRDDImpl[VD2, ED2] = {
    this.withPartitionsRDD[VD2, ED2](grapePartitionsRDD.mapPartitions({ iter =>
      if (iter.hasNext) {
        val (pid, ep) = iter.next()
        Iterator((pid, f(pid, ep)))
      } else {
        Iterator.empty
      }
    }, preservesPartitioning = true))
  }

  private[graphx] def withPartitionsRDD[VD2: ClassTag, ED2: ClassTag](
          partitionsRDD: RDD[(PartitionID, GrapeEdgePartition[VD2, ED2])]): GrapeEdgeRDDImpl[VD2, ED2] = {
    new GrapeEdgeRDDImpl[VD2,ED2](partitionsRDD, this.targetStorageLevel)
  }
}
